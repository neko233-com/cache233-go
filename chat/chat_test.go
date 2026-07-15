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

func TestStoreExtensionHooks(t *testing.T) {
	appends := 0
	s, err := NewWithConfig(Config[string, string, string]{ConversationCapacity: 2, MessagesPerConversation: 2, Accept: func(_ string, message Message[string, string]) bool { return message.Payload != "drop" }, OnAppend: func(_ string, _ Message[string, string], _ bool) { appends++ }})
	if err != nil {
		t.Fatal(err)
	}
	if s.Append("room", Message[string, string]{ID: "1", Payload: "drop"}) {
		t.Fatal("message should be rejected")
	}
	if !s.Append("room", Message[string, string]{ID: "2", Payload: "keep"}) || appends != 1 {
		t.Fatal("hook was not called")
	}
}
