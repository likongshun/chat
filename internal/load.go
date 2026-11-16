package config

import (
    "fmt"
    "strconv"
    "strings"

    "github.com/xuri/excelize/v2"
)

func LoadXLSX(path string) (*Snapshot, error) {
    f, err := excelize.OpenFile(path)
    if err != nil { return nil, fmt.Errorf("open workbook: %w", err) }
    defer f.Close()

    s := &Snapshot{}
    if err := loadDevices(f, s); err != nil { return nil, err }
    if err := loadPoints(f, s); err != nil { return nil, err }
    if err := loadProcessing(f, s); err != nil { return nil, err }
    if err := loadWriteRules(f, s); err != nil { return nil, err }
    if err := loadEnums(f, s); err != nil { return nil, err }
    if err := Validate(s); err != nil { return nil, err }
    return s, nil
}

func rows(f *excelize.File, sheet string) ([][]string, error) {
    r, err := f.GetRows(sheet)
    if err != nil { return nil, fmt.Errorf("sheet %s: %w", sheet, err) }
    if len(r) == 0 { return nil, fmt.Errorf("sheet %s is empty", sheet) }
    return r, nil
}
func cell(r []string, i int) string { if i >= len(r) { return "" }; return strings.TrimSpace(r[i]) }
func num(r []string, i int, def float64) (float64, error) {
    v := cell(r, i); if v == "" { return def, nil }
    n, err := strconv.ParseFloat(v, 64); if err != nil { return 0, fmt.Errorf("invalid number %q", v) }; return n, nil
}
func integer(r []string, i int, def int) (int, error) {
    n, err := num(r, i, float64(def)); return int(n), err
}
func boolean(r []string, i int, def bool) (bool, error) {
    v := strings.ToUpper(cell(r, i)); if v == "" { return def, nil }
    switch v { case "TRUE", "1", "YES": return true, nil; case "FALSE", "0", "NO": return false, nil }
    return false, fmt.Errorf("invalid boolean %q", v)
}
func dataRows(f *excelize.File, sheet string, minCols int) ([][]string, error) {
    rs, err := rows(f, sheet); if err != nil { return nil, err }
    if len(rs[0]) < minCols { return nil, fmt.Errorf("%s: expected at least %d columns", sheet, minCols) }
    return rs[1:], nil
}
func loadDevices(f *excelize.File, s *Snapshot) error {
    rs, err := dataRows(f, "Devices", 8); if err != nil { return err }
    for i,r := range rs { if cell(r,0)=="" { continue }
        port,e:=integer(r,3,502); if e!=nil{return fmt.Errorf("Devices row %d: %w",i+2,e)}
        uid,e:=integer(r,4,1); if e!=nil{return e}; poll,e:=integer(r,6,1000); if e!=nil{return e}
        en,e:=boolean(r,7,true); if e!=nil{return e}
        s.Devices=append(s.Devices,Device{cell(r,0),cell(r,1),cell(r,2),port,uint8(uid),cell(r,5),poll,en})
    }; return nil
}
func loadPoints(f *excelize.File, s *Snapshot) error {
    rs,err:=dataRows(f,"Points",15); if err!=nil{return err}
    for i,r:=range rs { if cell(r,0)=="" {continue}
        fn,e:=integer(r,3,3); if e!=nil{return fmt.Errorf("Points row %d: %w",i+2,e)}
        addr,e:=integer(r,4,0); if e!=nil{return e}; length,e:=integer(r,6,0); if e!=nil{return e}
        scale,e:=num(r,7,1); if e!=nil{return e}; off,e:=num(r,8,0); if e!=nil{return e}
        en,e:=boolean(r,14,true); if e!=nil{return e}
        s.Points=append(s.Points,Point{cell(r,0),cell(r,1),cell(r,2),fn,uint16(addr),strings.ToUpper(cell(r,5)),length,scale,off,strings.ToUpper(cell(r,9)),strings.ToUpper(cell(r,10)),cell(r,11),strings.ToUpper(cell(r,12)),en,cell(r,13)})
    }; return nil
}
func loadProcessing(f *excelize.File,s *Snapshot) error {
    rs,err:=dataRows(f,"Processing",4); if err!=nil{return err}
    for _,r:=range rs { if cell(r,0)=="" {continue}; a,e:=num(r,2,0);if e!=nil{return e};b,e:=num(r,3,0);if e!=nil{return e}
        s.Processing=append(s.Processing,Processing{cell(r,0),strings.ToUpper(cell(r,1)),a,b})
    }; return nil
}
func loadWriteRules(f *excelize.File,s *Snapshot) error {
    rs,err:=dataRows(f,"WriteRules",6);if err!=nil{return err}
    for _,r:=range rs {if cell(r,0)==""{continue}; en,e:=boolean(r,2,false);if e!=nil{return e}; mn,e:=num(r,3,0);if e!=nil{return e};mx,e:=num(r,4,0);if e!=nil{return e};cf,e:=boolean(r,5,true);if e!=nil{return e}
        s.WriteRules=append(s.WriteRules,WriteRule{cell(r,0),cell(r,1),en,mn,mx,cf})
    };return nil
}
func loadEnums(f *excelize.File,s *Snapshot) error {
    rs,err:=dataRows(f,"Enums",3);if err!=nil{return err}
    for _,r:=range rs {if cell(r,0)==""{continue};s.Enums=append(s.Enums,EnumValue{cell(r,0),cell(r,1),cell(r,2)})};return nil
}
