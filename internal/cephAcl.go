package internal

type AccessControlPolicy struct {
	Owner             Owner             `xml:"Owner"`
	AccessControlList AccessControlList `xml:"AccessControlList"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type AccessControlList struct {
	Grants []Grant `xml:"Grant"`
}

type Grant struct {
	Gruntee    Grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type Grantee struct {
	URI         string `xml:"URI"`
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}
