// SPDX-License-Identifier: GPL-3.0-or-later

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/borje/unframeo/internal/frameo"
	"github.com/borje/unframeo/internal/frameo/frameotest"
	"github.com/borje/unframeo/internal/frameo/pb"
)

func runCLIWithStdin(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runWithIO(args, strings.NewReader(stdin), &out)
	return out.String(), err
}

func pairTestFrame(t *testing.T, frame *frameotest.Frame) string {
	t.Helper()
	withNoConfig(t)
	server, code := startFakeFrame(t, frame)
	if _, err := runCLI(t, "-server", server, "-net", "relay", "pair", code); err != nil {
		t.Fatalf("pair: %v", err)
	}
	return server
}

func TestSendExplicitIDAndTakenJSON(t *testing.T) {
	frame := frameotest.New()
	server := pairTestFrame(t, frame)

	dir := t.TempDir()
	img := filepath.Join(dir, "sample.webp")
	if err := os.WriteFile(img, []byte("RIFFxxxxWEBP"), 0o600); err != nil {
		t.Fatal(err)
	}

	const wantID int64 = 0x4242000000007777
	const wantTakenMs int64 = 1712345678901

	out, err := runCLI(t,
		"-server", server, "-net", "relay",
		"-id", strconv.FormatInt(wantID, 10),
		"-taken", strconv.FormatInt(wantTakenMs, 10),
		"-caption", "May 2024 · Montreal",
		"-fit", "-json",
		"send", img,
	)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	var got jsonSent
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("parse json output %q: %v", out, err)
	}
	if got.ID != wantID || got.Path != img {
		t.Fatalf("got %+v, want id=%d path=%q", got, wantID, img)
	}

	photos := frame.Photos()
	if len(photos) != 1 {
		t.Fatalf("got %d photos on frame, want 1", len(photos))
	}
	m := photos[0].Media
	if m.GetId() != wantID || m.GetContentId() != wantID {
		t.Fatalf("frame media id=%d content_id=%d, want %d", m.GetId(), m.GetContentId(), wantID)
	}
	if m.GetCaptureDate() != wantTakenMs {
		t.Fatalf("capture_date=%d, want %d", m.GetCaptureDate(), wantTakenMs)
	}
	if m.GetCaption() != "May 2024 · Montreal" {
		t.Fatalf("caption=%q", m.GetCaption())
	}

	// -id with multiple paths must be rejected before sending.
	if _, err := runCLI(t, "-server", server, "-net", "relay", "-id", "123", "send", img, img); err == nil {
		t.Fatal("expected error when using -id with multiple files")
	}
}

func TestInfoAndListJSON(t *testing.T) {
	frame := frameotest.New()
	frame.ListType = frameo.TypeGetAllMediaMetaData
	server := pairTestFrame(t, frame)

	infoOut, err := runCLI(t, "-server", server, "-net", "relay", "-json", "info")
	if err != nil {
		t.Fatalf("info -json: %v", err)
	}
	var info jsonInfo
	if err := json.Unmarshal([]byte(infoOut), &info); err != nil {
		t.Fatalf("decode info json %q: %v", infoOut, err)
	}
	if info.Name != "Test Frame" || info.ScreenWidth != 1280 || info.ScreenHeight != 800 || info.Placement != "Living room" {
		t.Fatalf("unexpected info json: %+v", info)
	}

	emptyOut, err := runCLI(t, "-server", server, "-net", "relay", "-json", "list")
	if err != nil {
		t.Fatalf("empty list -json: %v", err)
	}
	if strings.TrimSpace(emptyOut) != "[]" {
		t.Fatalf("empty list json = %q, want []", strings.TrimSpace(emptyOut))
	}

	frame.Library = []*pb.MediaMetaData{
		{
			MediaId:     0x4242000000000001,
			Type:        0,
			CaptureDate: 1700000000000,
			ReceiveDate: 1700000500000,
			IsVisible:   true,
		},
	}
	listOut, err := runCLI(t, "-server", server, "-net", "relay", "-json", "list")
	if err != nil {
		t.Fatalf("list -json: %v", err)
	}
	var items []jsonMediaItem
	if err := json.Unmarshal([]byte(listOut), &items); err != nil {
		t.Fatalf("decode list json %q: %v", listOut, err)
	}
	if len(items) != 1 || items[0].MediaID != 0x4242000000000001 || items[0].Kind != "photo" || !items[0].IsVisible {
		t.Fatalf("unexpected list items: %+v", items)
	}
}

func TestSendBatch(t *testing.T) {
	frame := frameotest.New()
	server := pairTestFrame(t, frame)

	dir := t.TempDir()
	p1 := filepath.Join(dir, "one.webp")
	p2 := filepath.Join(dir, "two.webp")
	if err := os.WriteFile(p1, []byte("RIFF1111WEBP"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p2, []byte("RIFF2222WEBP"), 0o600); err != nil {
		t.Fatal(err)
	}

	b1, _ := json.Marshal(map[string]any{
		"ref":     "asset-1",
		"path":    p1,
		"id":      int64(5001),
		"taken":   int64(1700000001000),
		"caption": "First",
		"fit":     true,
	})
	b2, _ := json.Marshal(map[string]any{
		"ref":     "asset-2",
		"path":    p2,
		"id":      int64(5002),
		"taken":   "2024-06-01T12:00:00Z",
		"caption": "Second",
		"fit":     false,
	})
	bMissing, _ := json.Marshal(map[string]any{
		"ref":  "asset-missing",
		"path": filepath.Join(dir, "nope.webp"),
		"id":   int64(5003),
	})

	stdin := string(b1) + "\n\n" + string(b2) + "\n"
	out, err := runCLIWithStdin(t, stdin, "-server", server, "-net", "relay", "send-batch")
	if err != nil {
		t.Fatalf("send-batch: %v\noutput:\n%s", err, out)
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d output lines, want 2:\n%s", len(lines), out)
	}
	var r1, r2 batchResult
	if err := json.Unmarshal([]byte(lines[0]), &r1); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &r2); err != nil {
		t.Fatal(err)
	}
	if !r1.OK || r1.ID != 5001 || r1.Ref != "asset-1" {
		t.Fatalf("unexpected r1: %+v", r1)
	}
	if !r2.OK || r2.ID != 5002 || r2.Ref != "asset-2" {
		t.Fatalf("unexpected r2: %+v", r2)
	}

	photos := frame.Photos()
	if len(photos) != 2 {
		t.Fatalf("frame got %d photos, want 2", len(photos))
	}
	wantT2, _ := time.Parse(time.RFC3339, "2024-06-01T12:00:00Z")
	if photos[1].Media.GetCaptureDate() != wantT2.UnixMilli() {
		t.Fatalf("photo 2 capture_date=%d, want %d", photos[1].Media.GetCaptureDate(), wantT2.UnixMilli())
	}

	// Partial failure still emits JSON for both lines and exits non-zero.
	out, err = runCLIWithStdin(t, string(bMissing)+"\n"+string(b1)+"\n", "-server", server, "-net", "relay", "send-batch")
	if err == nil {
		t.Fatal("expected error when one batch item is missing")
	}
	lines = strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines after partial failure, want 2:\n%s", len(lines), out)
	}
	var failRes, okRes batchResult
	_ = json.Unmarshal([]byte(lines[0]), &failRes)
	_ = json.Unmarshal([]byte(lines[1]), &okRes)
	if failRes.OK || failRes.Error == "" || !okRes.OK {
		t.Fatalf("unexpected partial failure results: fail=%+v ok=%+v", failRes, okRes)
	}
}

func TestReactionsAndAckCLI(t *testing.T) {
	rx1 := frameo.Reaction{ID: 7001, MediaID: 5001, Type: 1, AcknowledgeID: 7001, TimestampMs: 1700000010000}
	rx2 := frameo.Reaction{ID: 7002, MediaID: 5002, Type: 2, AcknowledgeID: 7002, TimestampMs: 1700000020000}

	frame := frameotest.New()
	frame.QueuedReactions = []frameotest.QueuedReaction{
		{ID: int64(rx1.ID), Payload: frameo.EncodeReaction(rx1)},
		{ID: int64(rx2.ID), Payload: frameo.EncodeReaction(rx2)},
	}
	server := pairTestFrame(t, frame)

	// Drain without -ack leaves both queued on the frame.
	out, err := runCLI(t, "-server", server, "-net", "relay", "-wait", "30ms", "-json", "reactions")
	if err != nil {
		t.Fatalf("reactions: %v", err)
	}
	var got []frameo.Reaction
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("parse reactions json %q: %v", out, err)
	}
	if len(got) != 2 || got[0].Label != "heart" || got[1].Label != "laugh" {
		t.Fatalf("unexpected reactions: %+v", got)
	}
	if len(frame.RemainingReactions()) != 2 {
		t.Fatalf("reactions removed without ack: remaining=%d", len(frame.RemainingReactions()))
	}

	// Ack only rx1 via ack-reactions.
	if _, err := runCLI(t, "-server", server, "-net", "relay", "-json", "ack-reactions", "7001"); err != nil {
		t.Fatalf("ack-reactions: %v", err)
	}
	// Drain with -ack collects remaining rx2 and clears it.
	out, err = runCLI(t, "-server", server, "-net", "relay", "-wait", "30ms", "-ack", "-json", "reactions")
	if err != nil {
		t.Fatalf("reactions -ack: %v", err)
	}
	got = nil
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("parse reactions -ack json %q: %v", out, err)
	}
	if len(got) != 1 || got[0].ID != 7002 {
		t.Fatalf("expected only rx2 after partial ack, got %+v", got)
	}

	// Queue should now be empty.
	out, err = runCLI(t, "-server", server, "-net", "relay", "-wait", "30ms", "-json", "reactions")
	if err != nil {
		t.Fatalf("reactions after ack: %v", err)
	}
	if strings.TrimSpace(out) != "[]" {
		t.Fatalf("expected empty reaction array [], got %q", out)
	}

	// Verify ack-reactions also reads IDs from stdin.
	rx3 := frameo.Reaction{ID: 7003, MediaID: 5003, Type: 4, AcknowledgeID: 7003, TimestampMs: 1700000030000}
	frame.QueuedReactions = []frameotest.QueuedReaction{
		{ID: int64(rx3.ID), Payload: frameo.EncodeReaction(rx3)},
	}
	if _, err := runCLIWithStdin(t, "7003\n", "-server", server, "-net", "relay", "ack-reactions"); err != nil {
		t.Fatalf("ack-reactions stdin: %v", err)
	}
	if rem := frame.RemainingReactions(); len(rem) != 0 {
		t.Fatalf("expected 0 remaining reactions after stdin ack, got %d", len(rem))
	}
}
