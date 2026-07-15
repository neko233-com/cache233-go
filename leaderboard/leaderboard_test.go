package leaderboard

import "testing"

func TestBoardKeepsTopScores(t *testing.T) {
	b, err := New[string](2)
	if err != nil {
		t.Fatal(err)
	}
	for member, score := range map[string]float64{"a": 1, "b": 3, "c": 2} {
		if err := b.Set(member, score); err != nil {
			t.Fatal(err)
		}
	}
	top := b.Top(2)
	if len(top) != 2 || top[0].Member != "b" || top[1].Member != "c" {
		t.Fatalf("unexpected ranking: %#v", top)
	}
	if _, ok := b.RankOf("a"); ok {
		t.Fatal("lowest member should be pruned")
	}
}

func TestBoardExtensionHooks(t *testing.T) {
	changes := 0
	b, err := NewWithConfig(Config[string]{Capacity: 2, Normalize: func(_ string, score float64) (float64, error) {
		if score > 100 {
			return 100, nil
		}
		return score, nil
	}, OnChange: func(change Change[string]) {
		changes++
		if change.Score != 100 {
			t.Fatalf("unexpected score: %v", change.Score)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Set("a", 500); err != nil || changes != 1 {
		t.Fatal("extension hooks failed")
	}
}
