package chat

import "testing"

func TestStoreRecentAndIdempotentAppend(t *testing.T) {
	s, err := New[string, string, string](2, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.Append("room", Message[string, string]{ID: "1", Payload: "first"})
	s.Append("room", Message[string, string]{ID: "1", Payload: "updated"})
	s.Append("room", Message[string, string]{ID: "2"})
	s.Append("room", Message[string, string]{ID: "3"})
	got := s.Recent("room", 10)
	if len(got) != 2 || got[0].ID != "2" || got[1].ID != "3" {
		t.Fatalf("unexpected messages: %#v", got)
	}
}
