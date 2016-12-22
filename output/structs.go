package output

type Entry struct {
	Key     string   `json:"key,omitempty"`
	Buckets []Bucket `json:"buckets,omitempty"`
}

type Bucket struct {
	Timestamp   string `json:"timestamp,omitempty"`
	Nrequests   string `json:"n_requests,omitempty"`
	NreqUniq    string `json:"n_req_uniq,omitempty"`
	Nmisses     string `json:"n_misses,omitempty"`
	Nrestarts   string `json:"n_restarts,omitempty"`
	TTFBmiss    string `json:"ttfb_miss,omitempty"`
	TTFBhit     string `json:"ttfb_hit,omitempty"`
	NbodyBytes  string `json:"n_bodybytes,omitempty"`
	RespBytes   string `json:"respbytes,omitempty"`
	ReqBytes    string `json:"reqbytes,omitempty"`
	BeReqBytes  string `json:"bereqbytes,omitempty"`
	BeRespBytes string `json:"berespbytes,omitempty"`
	RespCode1xx string `json:"resp_code_1xx,omitempty"`
	RespCode2xx string `json:"resp_code_2xx,omitempty"`
	RespCode3xx string `json:"resp_code_3xx,omitempty"`
	RespCode4xx string `json:"resp_code_4xx,omitempty"`
	RespCode5xx string `json:"resp_code_5xx,omitempty"`
}

type Output interface {
	Add(Entry) error
	Setup(map[string]string) error
}

func New(output string) Output {
	switch output {
	default:
		return collectd.New()
	}
}
