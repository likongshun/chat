package dlt645

import "testing"

func TestDataIDTemplateCatalog(t *testing.T) {
	items := ListDataIDTemplates()
	if len(items) < 8 { t.Fatalf("catalog unexpectedly small: %d", len(items)) }
	item, ok := FindDataIDTemplate(" 00010000 ")
	if !ok || item.Name != "正向有功总电能" || item.Length != 4 { t.Fatalf("unexpected template: %+v, %v", item, ok) }
	if _, ok := FindDataIDTemplate("FFFFFFFF"); ok { t.Fatal("unexpected unknown template") }
	if err := ValidateDataIDTemplate(item); err != nil { t.Fatal(err) }
	item.ID = "GG000000"
	if err := ValidateDataIDTemplate(item); err == nil { t.Fatal("expected invalid data id") }
}

func TestTemplateListIsDefensiveCopy(t *testing.T) {
	items := ListDataIDTemplates()
	items[0].Name = "changed"
	for _, item := range ListDataIDTemplates() { if item.Name == "changed" { t.Fatal("catalog was mutated") } }
}
