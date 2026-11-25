// SPDX-License-Identifier: GPL-3.0-or-later

package frameo_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/borje/unframeo/internal/frameo"
	"github.com/borje/unframeo/internal/frameo/frameotest"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestDecodeReactionRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   frameo.Reaction
		want string
	}{
		{
			name: "heart",
			in: frameo.Reaction{
				ID:            8339213651806196088,
				MediaID:       0x4242000000000001,
				Type:          1,
				AcknowledgeID: 8339213651806196088,
				TimestampMs:   1795620000000,
			},
			want: "heart",
		},
		{
			name: "laugh_negative_zigzag_media_id",
			in: frameo.Reaction{
				ID:            99001,
				MediaID:       -42,
				Type:          2,
				AcknowledgeID: 99001,
				TimestampMs:   1795620001000,
			},
			want: "laugh",
		},
		{
			name: "surprised",
			in: frameo.Reaction{
				ID:            99002,
				MediaID:       123456789,
				Type:          4,
				AcknowledgeID: 99002,
				TimestampMs:   1795620002000,
			},
			want: "surprised",
		},
		{
			name: "cleared_omits_field_3",
			in: frameo.Reaction{
				ID:            99003,
				MediaID:       123456789,
				Type:          0,
				AcknowledgeID: 99003,
				TimestampMs:   1795620003000,
			},
			want: "cleared",
		},
		{
			name: "unknown_type_fallback",
			in: frameo.Reaction{
				ID:            99004,
				MediaID:       555,
				Type:          9,
				AcknowledgeID: 99004,
				TimestampMs:   1795620004000,
			},
			want: "reaction_9",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := frameo.EncodeReaction(tc.in)
			// Append an unknown length-delimited field to ensure forward compatibility.
			raw = protowire.AppendTag(raw, 99, protowire.BytesType)
			raw = protowire.AppendString(raw, "extra")

			got, err := frameo.DecodeReaction(raw)
			if err != nil {
				t.Fatalf("DecodeReaction: %v", err)
			}
			want := tc.in
			want.Label = tc.want
			if got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		})
	}

	if _, err := frameo.DecodeReaction([]byte{0x08, 0x80}); err == nil {
		t.Fatal("expected error for truncated varint")
	}
}

func TestDrainReactionsAndAck(t *testing.T) {
	for _, beforeInfo := range []bool{false, true} {
		name := "after_info"
		if beforeInfo {
			name = "before_info"
		}
		t.Run(name, func(t *testing.T) {
			r1 := frameo.Reaction{ID: 1001, MediaID: 0x424200000000000a, Type: 1, AcknowledgeID: 1001, TimestampMs: 1700000001000}
			r2 := frameo.Reaction{ID: 1002, MediaID: 0x424200000000000b, Type: 4, AcknowledgeID: 1002, TimestampMs: 1700000002000}

			frame := frameotest.New()
			frame.ReactionsBeforeInfo = beforeInfo
			frame.QueuedReactions = []frameotest.QueuedReaction{
				{ID: int64(r1.ID), Payload: frameo.EncodeReaction(r1)},
				{ID: int64(r2.ID), Payload: frameo.EncodeReaction(r2)},
			}
			c := setup(t, frame)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// First drain must receive both reactions without removing them from the frame.
			got, info, err := c.DrainReactions(ctx, 25*time.Millisecond)
			if err != nil {
				t.Fatalf("DrainReactions 1: %v", err)
			}
			if info.GetName() != "Test Frame" {
				t.Fatalf("got info name %q, want Test Frame", info.GetName())
			}
			if len(got) != 2 || got[0].Label != "heart" || got[1].Label != "surprised" {
				t.Fatalf("got reactions %+v", got)
			}
			if len(frame.RemainingReactions()) != 2 {
				t.Fatalf("reactions should stay queued until acked; remaining=%d", len(frame.RemainingReactions()))
			}

			// Ack only the first reaction; second should remain queued and reappear.
			if err := c.Acknowledge(ctx, []uint64{r1.AcknowledgeID}); err != nil {
				t.Fatalf("Acknowledge 1: %v", err)
			}
			got, _, err = c.DrainReactions(ctx, 25*time.Millisecond)
			if err != nil {
				t.Fatalf("DrainReactions 2: %v", err)
			}
			if len(got) != 1 || got[0].ID != r2.ID {
				t.Fatalf("expected only reaction %d after partial ack, got %+v", r2.ID, got)
			}

			// Ack the remaining reaction and confirm the queue is now empty.
			if err := c.Acknowledge(ctx, []uint64{r2.AcknowledgeID}); err != nil {
				t.Fatalf("Acknowledge 2: %v", err)
			}
			got, _, err = c.DrainReactions(ctx, 25*time.Millisecond)
			if err != nil {
				t.Fatalf("DrainReactions 3: %v", err)
			}
			if len(got) != 0 {
				t.Fatalf("expected empty queue after full ack, got %+v", got)
			}
		})
	}
}

func TestSendPhotoExplicitID(t *testing.T) {
	frame := frameotest.New()
	c := setup(t, frame)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p := filepath.Join(t.TempDir(), "photo.webp")
	if err := os.WriteFile(p, []byte("RIFFxxxxWEBP"), 0o600); err != nil {
		t.Fatal(err)
	}

	const wantID int64 = 0x4242000000001234
	taken := time.UnixMilli(1700000123456)
	gotID, err := c.SendPhoto(ctx, frameo.Photo{
		ID:      wantID,
		Path:    p,
		Taken:   taken,
		Caption: "explicit id test",
		Fit:     true,
	})
	if err != nil {
		t.Fatalf("SendPhoto: %v", err)
	}
	if gotID != wantID {
		t.Fatalf("returned id = %d, want %d", gotID, wantID)
	}
	received := frame.Photos()
	if len(received) != 1 {
		t.Fatalf("received %d photos, want 1", len(received))
	}
	m := received[0].Media
	if m.GetId() != wantID || m.GetContentId() != wantID {
		t.Fatalf("frame saw id=%d content_id=%d, want %d", m.GetId(), m.GetContentId(), wantID)
	}
	if m.GetCaptureDate() != taken.UnixMilli() {
		t.Fatalf("frame saw capture_date=%d, want %d", m.GetCaptureDate(), taken.UnixMilli())
	}
}
