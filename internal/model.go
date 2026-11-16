package config

type Device struct {
    DeviceID string
    Name string
    Host string
    Port int
    UnitID uint8
    Model string
    PollMS int
    Enabled bool
}

type Point struct {
    Model string
    PointCode string
    Name string
    Function int
    Address uint16
    DataType string
    Length int
    Scale float64
    Offset float64
    ByteOrder string
    WordOrder string
    Unit string
    RW string
    Enabled bool
    ReadGroup string
}

type Processing struct {
    PointCode string
    Operation string
    Param1 float64
    Param2 float64
}

type WriteRule struct {
    Model string
    PointCode string
    Enabled bool
    Min float64
    Max float64
    RequireConfirm bool
}

type EnumValue struct {
    PointCode string
    Value string
    Label string
}

type Snapshot struct {
    Devices []Device
    Points []Point
    Processing []Processing
    WriteRules []WriteRule
    Enums []EnumValue
}
