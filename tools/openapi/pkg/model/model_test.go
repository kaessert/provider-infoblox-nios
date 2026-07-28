package model

import "testing"

func TestResourceHasFullCRUD(t *testing.T) {
	cases := []struct {
		name string
		crud []CRUDMethod
		want bool
	}{
		{
			name: "FullCRUD",
			crud: []CRUDMethod{
				{Operation: "Create", Method: "CreateARecord"},
				{Operation: "Read", Method: "GetARecord"},
				{Operation: "Update", Method: "UpdateARecord"},
				{Operation: "Delete", Method: "DeleteARecord"},
			},
			want: true,
		},
		{
			name: "MissingUpdate",
			crud: []CRUDMethod{
				{Operation: "Create", Method: "CreateZoneAuth"},
				{Operation: "Read", Method: "GetZoneAuthByRef"},
				{Operation: "Delete", Method: "DeleteZoneAuth"},
			},
			want: false,
		},
		{
			name: "Empty",
			crud: nil,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := Resource{CRUD: tc.crud}
			if got := r.HasFullCRUD(); got != tc.want {
				t.Errorf("HasFullCRUD() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResourceFieldCounts(t *testing.T) {
	r := Resource{
		Fields: []Field{
			{JSONName: "name", Scope: FieldScopeRequest},
			{JSONName: "comment", Scope: FieldScopeBoth},
			{JSONName: "_ref", Scope: FieldScopeResponse},
			{JSONName: "creation_time", Scope: FieldScopeResponse},
			{JSONName: "view", Scope: FieldScopeBoth},
		},
	}
	req, resp, both := r.FieldCounts()
	if req != 1 {
		t.Errorf("req = %d, want 1", req)
	}
	if resp != 2 {
		t.Errorf("resp = %d, want 2", resp)
	}
	if both != 2 {
		t.Errorf("both = %d, want 2", both)
	}
}

func TestResourceUpdateMethod(t *testing.T) {
	r := Resource{CRUD: []CRUDMethod{
		{Operation: "Create", Method: "CreateARecord"},
		{Operation: "Update", Method: "UpdateARecord"},
	}}
	if got := r.UpdateMethod(); got != "UpdateARecord" {
		t.Errorf("UpdateMethod() = %q, want %q", got, "UpdateARecord")
	}

	none := Resource{CRUD: []CRUDMethod{{Operation: "Create", Method: "CreateZoneAuth"}}}
	if got := none.UpdateMethod(); got != "" {
		t.Errorf("UpdateMethod() = %q, want empty", got)
	}
}

func TestResourceDeleteMethod(t *testing.T) {
	r := Resource{CRUD: []CRUDMethod{
		{Operation: "Delete", Method: "DeleteARecord"},
	}}
	if got := r.DeleteMethod(); got != "DeleteARecord" {
		t.Errorf("DeleteMethod() = %q, want %q", got, "DeleteARecord")
	}

	none := Resource{}
	if got := none.DeleteMethod(); got != "" {
		t.Errorf("DeleteMethod() = %q, want empty", got)
	}
}
