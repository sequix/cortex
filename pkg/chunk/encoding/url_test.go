package encoding

import (
	"testing"
)

func TestRawUrlEncode(t *testing.T) {
	testRawUrlEncode(t, "", "")
	testRawUrlEncode(t, " ", "%20")
	testRawUrlEncode(t, "123", "123")
	testRawUrlEncode(t, "<>", "%3C%3E")
	testRawUrlEncode(t, "\x00", "%00")
}

func testRawUrlEncode(t *testing.T, src, want string) {
	t.Helper()

	got := RawUrlEncode(src)
	if got != want {
		t.Errorf("rawUrlEncode %q got %q, want %q", src, got, want)
	}
}

func TestRawUrlDeocde(t *testing.T) {
	testRawUrlDecode(t, "", "")
	testRawUrlDecode(t, "%20", " ")
	testRawUrlDecode(t, "123", "123")
	testRawUrlDecode(t, "%3C%3E", "<>")
	testRawUrlDecode(t, "%00", "\x00")
}

func testRawUrlDecode(t *testing.T, src, want string) {
	t.Helper()

	got := RawUrlDecode(src)
	if got != want {
		t.Errorf("rawUrlDecode %q got %q, want %q", src, got, want)
	}
}
