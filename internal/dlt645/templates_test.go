package dlt645

import "testing"

func TestDataIDTemplateCatalog(t *testing.T) {
	items := ListDataIDTemplates()
	if len(items) < 20 { t.Fatalf("catalog unexpectedly small: %d", len(items)) }
	item, ok := FindDataIDTemplate(" 00010000 ")
	if !ok || item.Name != "正向有功总电能" || item.Length != 4 { t.Fatalf("unexpected template: %+v, %v", item, ok) }
	if _, ok := FindDataIDTemplate("FFFFFFFF"); ok { t.Fatal("unexpected unknown template") }
	if err := ValidateDataIDTemplate(item); err != nil { t.Fatal(err) }
	for _, id := range []string{"01010000", "02030100", "04000101", "04000102", "04000204", "05000001", "05000101", "05000901", "05000A01"} {
		if _, ok := FindDataIDTemplate(id); !ok { t.Fatalf("missing standard DI template %s", id) }
	}
	freezeDemand, ok := FindDataIDTemplate("05000901")
	if !ok || freezeDemand.Length != 8 || freezeDemand.DataFormat != "XX.XXXX+YYMMDDHHMM" { t.Fatalf("unexpected frozen-demand template: %+v", freezeDemand) }
	if err := ValidateDataIDTemplate(freezeDemand); err != nil { t.Fatal(err) }
	item.ID = "GG000000"
	if err := ValidateDataIDTemplate(item); err == nil { t.Fatal("expected invalid data id") }
}

func TestTemplateListIsDefensiveCopy(t *testing.T) {
	items := ListDataIDTemplates()
	items[0].Name = "changed"
	for _, item := range ListDataIDTemplates() { if item.Name == "changed" { t.Fatal("catalog was mutated") } }
}
