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

const defaultCLISettle = time.Second

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
	ID        int64           `json:"id,omitempty"`
	Taken     json.RawMessage `json:"taken,omitempty"`
	Caption   string          `json:"caption,omitempty"`
	Fit       *bool           `json:"fit,omitempty"`
	Extension string          `json:"extension,omitempty"`
}

type batchResult struct {
	Ref   string `json:"ref,omitempty"`
	Path  string `json:"path"`
	ID    int64  `json:"id,omitempty"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func cmdSendBatch(ctx context.Context, cfg *config.Config, o *options) error {
	reader := bufio.NewReader(o.in)
	var (
		c      *frameo.Client
		total  int
		failed int
		lineNo int
	)
	defer func() {
		if c != nil {
			_ = c.Close()
		}
	}()

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("reading send-batch input: %w", err)
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			lineNo++
			total++
			var item batchItem
			if uerr := json.Unmarshal(trimmed, &item); uerr != nil {
				return fmt.Errorf("send-batch line %d: %w", lineNo, uerr)
			}
			sendErr, fatal := processBatchItem(ctx, cfg, o, &c, item)
			if fatal != nil {
				return fatal
			}
			if sendErr != nil {
				failed++
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}

	if failed > 0 {
		return fmt.Errorf("%d of %d batch items failed", failed, total)
	}
	return nil
}

func processBatchItem(ctx context.Context, cfg *config.Config, o *options, cp **frameo.Client, item batchItem) (itemErr, fatalErr error) {
	res := batchResult{Ref: item.Ref, Path: item.Path, ID: item.ID}
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
	if len(bytes.TrimSpace(item.Taken)) > 0 {
		parsed, err := parseTakenJSON(item.Taken)
		if err != nil {
			if werr := emitErr(err); werr != nil {
				return nil, werr
			}
			return err, nil
		}
		taken = parsed
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
		ID:            item.ID,
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
	res.OK = true
	if werr := writeJSON(o.out, res); werr != nil {
		return nil, werr
	}
	return nil, nil
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
