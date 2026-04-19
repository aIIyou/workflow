package util

import "testing"

func TestPascal(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
		{
			name: "test1",
			args: args{
				s: "syntax_check",
			},
			want: "SyntaxCheck",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Pascal(tt.args.s); got != tt.want {
				t.Errorf("Pascal() = %v, want %v", got, tt.want)
			}
		})
	}
}
