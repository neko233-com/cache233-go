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
