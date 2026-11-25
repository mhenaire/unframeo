// SPDX-License-Identifier: GPL-3.0-or-later

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/borje/unframeo/internal/config"
	"github.com/borje/unframeo/internal/frameo"
	"github.com/borje/unframeo/internal/frameo/pb"
)

const (
	defaultCLISettle = time.Second
	// maxBatchItems bounds single-connection batch operations to keep frame
	// transfers and protocol requests bounded.
	maxBatchItems = 100
)

var subcommandHelp = map[string]string{
	"send-batch": `Usage: unframeo [options] send-batch

Reads newline-delimited JSON (JSONL) upload descriptors from standard input
and sends them over a single connection to the frame, writing one JSON result
line per item to standard output.

Descriptor fields:
  path        file path to upload (required)
  media_id    optional explicit 64-bit photo id (alias: id)
  taken       optional capture time (Unix milliseconds or RFC3339 string)
  caption     optional caption overriding -caption
  fit         optional bool overriding -fit
  extension   optional format override
  ref         optional opaque caller reference echoed in results

Limit: maximum 100 items per batch; batches containing more than 100 items are
rejected before opening a connection.
`,
	"delete-batch": `Usage: unframeo [options] delete-batch

Reads newline-delimited media IDs (integers) or JSONL objects
({"media_id": <int64>, "ref": "..."}; alias: "id") from standard input and
deletes them over a single connection to the frame, writing one JSON result
line per item to standard output.

Limit: maximum 100 items per batch; batches containing more than 100 items are
rejected before opening a connection.
`,
	"hide-batch": `Usage: unframeo [options] hide-batch

Reads newline-delimited media IDs (integers) or JSONL objects
({"media_id": <int64>, "ref": "..."}; alias: "id") from standard input and
hides them over a single connection to the frame, writing one JSON result line
per item to standard output.

Limit: maximum 100 items per batch; batches containing more than 100 items are
rejected before opening a connection.
`,
}

func wantsHelp(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "--help" {
			return true
		}
	}
	return false
}

func parseTaken(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		if ms == 0 {
			return time.Time{}, nil
		}
		return time.UnixMilli(ms).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("-taken %q: expected Unix milliseconds or RFC3339 timestamp", s)
}

func parseTakenJSON(raw json.RawMessage) (time.Time, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return time.Time{}, nil
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return time.Time{}, err
		}
		return parseTaken(s)
	}
	var ms int64
	if err := json.Unmarshal(raw, &ms); err != nil {
		return time.Time{}, fmt.Errorf("taken %s: expected integer Unix milliseconds or RFC3339 string", string(raw))
	}
	if ms == 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(ms).UTC(), nil
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

type jsonInfo struct {
	Frame          string `json:"frame"`
	Via            string `json:"via"`
	Name           string `json:"name"`
	Placement      string `json:"placement,omitempty"`
	ScreenWidth    int32  `json:"screen_width"`
	ScreenHeight   int32  `json:"screen_height"`
	MayView        bool   `json:"may_view"`
	MayManage      bool   `json:"may_manage"`
	MaxVideoWidth  int32  `json:"max_video_width,omitempty"`
	MaxVideoHeight int32  `json:"max_video_height,omitempty"`
}

func writeJSONInfo(w io.Writer, frame *link, info *pb.FrameInfo) error {
	out := jsonInfo{
		Frame:        frame.name,
		Via:          frame.via,
		Name:         info.GetName(),
		Placement:    info.GetPlacement(),
		ScreenWidth:  info.GetScreenWidth(),
		ScreenHeight: info.GetScreenHeight(),
		MayView:      info.GetHasPermissionViewPhotos(),
		MayManage:    info.GetHasPermissionManagePhotos(),
	}
	if cap := info.GetFrameCapabilities(); cap != nil {
		out.MaxVideoWidth = cap.GetMaxVideoWidth()
		out.MaxVideoHeight = cap.GetMaxVideoHeight()
	}
	return writeJSON(w, out)
}

type jsonSent struct {
	Frame string `json:"frame"`
	Path  string `json:"path"`
	ID    int64  `json:"id"`
}

func writeJSONSent(w io.Writer, frameName, path string, id int64) error {
	return writeJSON(w, jsonSent{
		Frame: frameName,
		Path:  path,
		ID:    id,
	})
}

type jsonMediaItem struct {
	MediaID       int64  `json:"media_id"`
	Type          int32  `json:"type"`
	Kind          string `json:"kind"`
	CaptureDateMs int64  `json:"capture_date_ms"`
	ReceiveDateMs int64  `json:"receive_date_ms"`
	IsVisible     bool   `json:"is_visible"`
}

func writeJSONList(w io.Writer, items []*pb.MediaMetaData) error {
	out := make([]jsonMediaItem, 0, len(items))
	for _, m := range items {
		out = append(out, jsonMediaItem{
			MediaID:       m.GetMediaId(),
			Type:          int32(m.GetType()),
			Kind:          mediaKind(m.GetType()),
			CaptureDateMs: m.GetCaptureDate(),
			ReceiveDateMs: m.GetReceiveDate(),
			IsVisible:     m.GetIsVisible(),
		})
	}
	return writeJSON(w, out)
}

type batchItem struct {
	Ref       string          `json:"ref,omitempty"`
	Path      string          `json:"path"`
	ID        *int64          `json:"id,omitempty"`
	MediaID   *int64          `json:"media_id,omitempty"`
	Taken     json.RawMessage `json:"taken,omitempty"`
	Caption   string          `json:"caption,omitempty"`
	Fit       *bool           `json:"fit,omitempty"`
	Extension string          `json:"extension,omitempty"`
}

type batchIDItem struct {
	Ref      string `json:"ref,omitempty"`
	ID       *int64 `json:"id,omitempty"`
	MediaID  *int64 `json:"media_id,omitempty"`
	resolved int64
}

type batchResult struct {
	Ref     string `json:"ref,omitempty"`
	Path    string `json:"path,omitempty"`
	ID      int64  `json:"id,omitempty"`
	MediaID int64  `json:"media_id,omitempty"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

func resolveBatchID(id, mediaID *int64) (int64, error) {
	if id != nil && mediaID != nil && *id != *mediaID {
		return 0, fmt.Errorf("conflicting id (%d) and media_id (%d)", *id, *mediaID)
	}
	if mediaID != nil {
		return *mediaID, nil
	}
	if id != nil {
		return *id, nil
	}
	return 0, nil
}

// readBatchLines collects non-empty trimmed lines from r up to maxBatchItems.
// Returning an error as soon as item maxBatchItems+1 appears ensures oversized
// batches fail cleanly before connecting to a frame.
func readBatchLines(r io.Reader, cmdName string) ([][]byte, error) {
	reader := bufio.NewReader(r)
	var lines [][]byte
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("reading %s input: %w", cmdName, err)
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			if len(lines) >= maxBatchItems {
				return nil, fmt.Errorf("%s: batch exceeds maximum of %d items (rejected before connecting)", cmdName, maxBatchItems)
			}
			lines = append(lines, trimmed)
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return lines, nil
}

type parsedSendItem struct {
	raw   batchItem
	id    int64
	taken time.Time
}

func parseSendBatch(r io.Reader) ([]parsedSendItem, error) {
	lines, err := readBatchLines(r, "send-batch")
	if err != nil {
		return nil, err
	}
	items := make([]parsedSendItem, 0, len(lines))
	for i, line := range lines {
		var item batchItem
		if err := json.Unmarshal(line, &item); err != nil {
			return nil, fmt.Errorf("send-batch line %d: %w", i+1, err)
		}
		id, err := resolveBatchID(item.ID, item.MediaID)
		if err != nil {
			return nil, fmt.Errorf("send-batch line %d: %w", i+1, err)
		}
		var taken time.Time
		if len(bytes.TrimSpace(item.Taken)) > 0 {
			taken, err = parseTakenJSON(item.Taken)
			if err != nil {
				return nil, fmt.Errorf("send-batch line %d: %w", i+1, err)
			}
		}
		items = append(items, parsedSendItem{raw: item, id: id, taken: taken})
	}
	return items, nil
}

func cmdSendBatch(ctx context.Context, cfg *config.Config, o *options, args []string) error {
	if len(args) > 0 {
		return errors.New("usage: unframeo send-batch")
	}
	items, err := parseSendBatch(o.in)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	var (
		c      *frameo.Client
		failed int
	)
	defer func() {
		if c != nil {
			_ = c.Close()
		}
	}()

	for _, entry := range items {
		sendErr, fatal := processBatchItem(ctx, cfg, o, &c, entry)
		if fatal != nil {
			return fatal
		}
		if sendErr != nil {
			failed++
		}
	}

	if failed > 0 {
		return fmt.Errorf("%d of %d batch items failed", failed, len(items))
	}
	return nil
}

func processBatchItem(ctx context.Context, cfg *config.Config, o *options, cp **frameo.Client, entry parsedSendItem) (itemErr, fatalErr error) {
	item := entry.raw
	res := batchResult{Ref: item.Ref, Path: item.Path, ID: entry.id, MediaID: entry.id}
	emitErr := func(err error) error {
		res.OK = false
		res.Error = err.Error()
		return writeJSON(o.out, res)
	}

	if strings.TrimSpace(item.Path) == "" {
		err := errors.New("missing path")
		if werr := emitErr(err); werr != nil {
			return nil, werr
		}
		return err, nil
	}
	if err := checkPhotoFile(item.Path); err != nil {
		if werr := emitErr(err); werr != nil {
			return nil, werr
		}
		return err, nil
	}

	taken := o.taken
	if !entry.taken.IsZero() {
		taken = entry.taken
	}

	if *cp == nil {
		c, _, err := connect(ctx, cfg, o)
		if err != nil {
			_ = emitErr(err)
			return err, err
		}
		*cp = c
	}

	caption := item.Caption
	if caption == "" {
		caption = o.caption
	}
	fit := o.fit
	if item.Fit != nil {
		fit = *item.Fit
	}

	id, err := (*cp).SendPhoto(ctx, frameo.Photo{
		ID:            entry.id,
		Path:          item.Path,
		Extension:     item.Extension,
		Taken:         taken,
		Caption:       caption,
		Fit:           fit,
		SingleSegment: o.singleSegment,
	})
	if err != nil {
		if werr := emitErr(err); werr != nil {
			return nil, werr
		}
		// Frame rejections keep the connection usable for subsequent photos;
		// transport or context failures terminate the batch.
		var fe *frameo.FrameError
		if errors.As(err, &fe) {
			return err, nil
		}
		return err, err
	}

	res.ID = id
	res.MediaID = id
	res.OK = true
	if werr := writeJSON(o.out, res); werr != nil {
		return nil, werr
	}
	return nil, nil
}

func parseBatchIDs(r io.Reader, cmdName string) ([]batchIDItem, error) {
	lines, err := readBatchLines(r, cmdName)
	if err != nil {
		return nil, err
	}
	items := make([]batchIDItem, 0, len(lines))
	for i, line := range lines {
		var item batchIDItem
		if line[0] == '{' {
			if err := json.Unmarshal(line, &item); err != nil {
				return nil, fmt.Errorf("%s line %d: %w", cmdName, i+1, err)
			}
			id, err := resolveBatchID(item.ID, item.MediaID)
			if err != nil {
				return nil, fmt.Errorf("%s line %d: %w", cmdName, i+1, err)
			}
			if id == 0 {
				return nil, fmt.Errorf("%s line %d: missing media_id", cmdName, i+1)
			}
			item.resolved = id
		} else {
			id, err := strconv.ParseInt(string(line), 10, 64)
			if err != nil || id == 0 {
				return nil, fmt.Errorf("%s line %d: invalid media_id %q", cmdName, i+1, string(line))
			}
			item.resolved = id
		}
		items = append(items, item)
	}
	return items, nil
}

func cmdDeleteBatch(ctx context.Context, cfg *config.Config, o *options, args []string) error {
	return runIDBatch(ctx, cfg, o, "delete-batch", args, func(c *frameo.Client, ids []int64) error {
		return c.DeleteMedia(ctx, ids)
	})
}

func cmdHideBatch(ctx context.Context, cfg *config.Config, o *options, args []string) error {
	return runIDBatch(ctx, cfg, o, "hide-batch", args, func(c *frameo.Client, ids []int64) error {
		return c.SetMediaVisible(ctx, ids, false)
	})
}

func runIDBatch(ctx context.Context, cfg *config.Config, o *options, cmdName string, args []string, op func(*frameo.Client, []int64) error) error {
	if len(args) > 0 {
		return fmt.Errorf("usage: unframeo %s", cmdName)
	}
	items, err := parseBatchIDs(o.in, cmdName)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}

	c, _, err := connect(ctx, cfg, o)
	if err != nil {
		return err
	}
	defer c.Close()

	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.resolved
	}
	opErr := op(c, ids)
	errStr := ""
	if opErr != nil {
		errStr = opErr.Error()
	}
	for _, item := range items {
		if werr := writeJSON(o.out, batchResult{
			Ref:     item.Ref,
			ID:      item.resolved,
			MediaID: item.resolved,
			OK:      opErr == nil,
			Error:   errStr,
		}); werr != nil {
			return werr
		}
	}
	return opErr
}

func cmdReactions(ctx context.Context, cfg *config.Config, o *options) error {
	settle := o.wait
	if settle <= 0 {
		settle = defaultCLISettle
	}

	c, frame, err := connect(ctx, cfg, o)
	if err != nil {
		return err
	}
	defer c.Close()

	reactions, _, err := c.DrainReactions(ctx, settle)
	if err != nil {
		return err
	}
	if o.ack && len(reactions) > 0 {
		ids := make([]uint64, 0, len(reactions))
		for _, r := range reactions {
			ackID := r.AcknowledgeID
			if ackID == 0 {
				ackID = r.ID
			}
			ids = append(ids, ackID)
		}
		if err := c.Acknowledge(ctx, ids); err != nil {
			return err
		}
	}

	if o.jsonOut {
		if reactions == nil {
			reactions = []frameo.Reaction{}
		}
		return writeJSON(o.out, reactions)
	}
	if len(reactions) == 0 {
		fmt.Fprintf(o.out, "No queued reactions on %s.\n", frame)
		return nil
	}
	fmt.Fprintf(o.out, "%d queued reaction(s) on %s:\n", len(reactions), frame)
	fmt.Fprintf(o.out, "  %-20s  %-20s  %-10s  %s\n", "id", "media_id", "reaction", "time")
	for _, r := range reactions {
		fmt.Fprintf(o.out, "  %-20d  %-20d  %-10s  %s\n",
			r.ID, r.MediaID, r.Label, listedTime(r.TimestampMs))
	}
	return nil
}

func cmdAckReactions(ctx context.Context, cfg *config.Config, o *options, args []string) error {
	rawIDs := args
	if len(rawIDs) == 0 {
		data, err := io.ReadAll(o.in)
		if err != nil {
			return fmt.Errorf("reading reaction IDs: %w", err)
		}
		rawIDs = strings.Fields(string(data))
	}
	if len(rawIDs) == 0 {
		return errors.New("usage: unframeo ack-reactions <id>...")
	}

	ids := make([]uint64, 0, len(rawIDs))
	for _, s := range rawIDs {
		id, err := strconv.ParseUint(s, 0, 64)
		if err != nil {
			return fmt.Errorf("reaction id %q: %w", s, err)
		}
		ids = append(ids, id)
	}

	c, frame, err := connect(ctx, cfg, o)
	if err != nil {
		return err
	}
	defer c.Close()

	if err := c.Acknowledge(ctx, ids); err != nil {
		return err
	}
	if o.jsonOut {
		return writeJSON(o.out, struct {
			Frame        string   `json:"frame"`
			Acknowledged []uint64 `json:"acknowledged"`
		}{
			Frame:        frame.name,
			Acknowledged: ids,
		})
	}
	fmt.Fprintf(o.out, "Acknowledged %d reaction(s) on %s.\n", len(ids), frame)
	return nil
}
