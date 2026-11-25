// SPDX-License-Identifier: GPL-3.0-or-later

package frameo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	pb "github.com/borje/unframeo/internal/frameo/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const defaultReactionSettle = 500 * time.Millisecond

// Reaction represents a photo reaction queued by a frame and pushed to
// connected clients as a TypeReaction (10) message.
type Reaction struct {
	ID            uint64 `json:"id"`
	MediaID       int64  `json:"media_id"`
	Type          int32  `json:"type"`
	Label         string `json:"label"`
	AcknowledgeID uint64 `json:"acknowledge_id"`
	TimestampMs   int64  `json:"timestamp_ms"`
}

// ReactionLabel returns the human-readable name for a Frameo reaction enum value.
// Observed live values on Frameo hardware: 0 = cleared, 1 = heart, 2 = laugh,
// 4 = surprised. Unseen values fall back to "reaction_<n>".
func ReactionLabel(t int32) string {
	switch t {
	case 0:
		return "cleared"
	case 1:
		return "heart"
	case 2:
		return "laugh"
	case 4:
		return "surprised"
	default:
		return fmt.Sprintf("reaction_%d", t)
	}
}

// DecodeReaction parses a TypeReaction (10) protobuf payload without requiring
// generated stubs for the undocumented message shape.
func DecodeReaction(payload []byte) (Reaction, error) {
	var r Reaction
	b := payload
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return Reaction{}, fmt.Errorf("frameo: malformed reaction tag: %w", protowire.ParseError(n))
		}
		b = b[n:]

		switch num {
		case 1, 2, 3, 5, 6:
			if typ != protowire.VarintType {
				m := protowire.ConsumeFieldValue(num, typ, b)
				if m < 0 {
					return Reaction{}, fmt.Errorf("frameo: malformed reaction field %d: %w", num, protowire.ParseError(m))
				}
				b = b[m:]
				continue
			}
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return Reaction{}, fmt.Errorf("frameo: malformed reaction varint field %d: %w", num, protowire.ParseError(m))
			}
			b = b[m:]
			switch num {
			case 1:
				r.ID = v
			case 2:
				r.MediaID = protowire.DecodeZigZag(v)
			case 3:
				r.Type = int32(v)
			case 5:
				r.AcknowledgeID = v
			case 6:
				r.TimestampMs = int64(v)
			}
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return Reaction{}, fmt.Errorf("frameo: malformed reaction field %d: %w", num, protowire.ParseError(m))
			}
			b = b[m:]
		}
	}
	r.Label = ReactionLabel(r.Type)
	return r, nil
}

// EncodeReaction serializes r into the TypeReaction (10) protobuf wire shape.
func EncodeReaction(r Reaction) []byte {
	var b []byte
	ackID := r.AcknowledgeID
	if ackID == 0 {
		ackID = r.ID
	}
	if r.ID != 0 {
		b = protowire.AppendTag(b, 1, protowire.VarintType)
		b = protowire.AppendVarint(b, r.ID)
	}
	if r.MediaID != 0 {
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		b = protowire.AppendVarint(b, protowire.EncodeZigZag(r.MediaID))
	}
	if r.Type != 0 {
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(uint32(r.Type)))
	}
	if ackID != 0 {
		b = protowire.AppendTag(b, 5, protowire.VarintType)
		b = protowire.AppendVarint(b, ackID)
	}
	if r.TimestampMs != 0 {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(r.TimestampMs))
	}
	return b
}

// SendAck sends a TypeAcknowledgeReceipt (6) message to the frame for one
// queued message ID (for example, Reaction.AcknowledgeID or Reaction.ID).
func (c *Client) SendAck(ctx context.Context, id uint64) error {
	return c.send(ctx, TypeAcknowledgeReceipt, &pb.AcknowledgeReceipt{AcknowledgeId: int64(id)})
}

// Acknowledge sends a TypeAcknowledgeReceipt (6) message for each ID in order.
func (c *Client) Acknowledge(ctx context.Context, ids []uint64) error {
	for _, id := range ids {
		if err := c.SendAck(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// DrainReactions prompts the frame with GetInfo(1), which causes Frameo to flush
// any queued TypeReaction (10) messages, and collects reactions without
// acknowledging them until settle elapses after FrameInfo and the last reaction.
func (c *Client) DrainReactions(ctx context.Context, settle time.Duration) ([]Reaction, *pb.FrameInfo, error) {
	if settle <= 0 {
		settle = defaultReactionSettle
	}
	if err := c.send(ctx, TypeGetInfo, &pb.GetInfo{}); err != nil {
		return nil, nil, err
	}

	var (
		reactions []Reaction
		info      *pb.FrameInfo
		gotInfo   bool
		timer     *time.Timer
		settleCh  <-chan time.Time
	)
	resetTimer := func() {
		if timer == nil {
			timer = time.NewTimer(settle)
			settleCh = timer.C
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(settle)
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				err := c.closedErr()
				if gotInfo && errors.Is(err, io.EOF) {
					return reactions, info, nil
				}
				return nil, nil, fmt.Errorf("frameo: draining reactions: %w", err)
			}
			switch f.Type {
			case TypeFrameInfo:
				var fi pb.FrameInfo
				if err := proto.Unmarshal(f.Payload, &fi); err != nil {
					return nil, nil, fmt.Errorf("frameo: malformed frame information: %w", err)
				}
				info = &fi
				gotInfo = true
				resetTimer()
			case TypeReaction:
				r, err := DecodeReaction(f.Payload)
				if err != nil {
					return nil, nil, err
				}
				reactions = append(reactions, r)
				if gotInfo {
					resetTimer()
				}
			default:
				c.log.Debug("ignoring an unrelated message while waiting", "for", "reactions", "got", f.String())
			}
		case <-settleCh:
			return reactions, info, nil
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("frameo: draining reactions: %w", ctx.Err())
		}
	}
}
