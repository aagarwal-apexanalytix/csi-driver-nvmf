package nvmf

import (
	"strings"

	"k8s.io/klog/v2"
)

// MikroTik "compress" is yes/no. We map common values into yes/no.
func getEffectiveCompress(params map[string]string) string {
	if val, ok := params["compression"]; ok {
		val = strings.ToLower(strings.TrimSpace(val))
		switch val {
		case "", "off", "none", "no", "false", "disabled":
			return "no"
		case "on", "yes", "true", "enabled", "zstd", "zstd:1", "zstd:2", "zstd:3", "zstd:4",
			"zstd:5", "zstd:6", "zstd:7", "zstd:8", "zstd:9", "zstd:10", "zstd:11",
			"zstd:12", "zstd:13", "zstd:14", "zstd:15", "lzo", "zlib":
			return "yes"
		default:
			klog.Warningf("Invalid compression value %q → defaulting to compress=no", val)
		}
	}
	klog.V(4).Infof("No compression parameter specified → defaulting to compress=no")
	return "no"
}

// nvme-proxy: pass through zstd / zstd:3 / off etc. If user gave "yes", map to "zstd".
func nvmeProxyCompressFromParams(params map[string]string) string {
	raw, ok := params["compression"]
	if !ok {
		return ""
	}
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return ""
	}
	switch v {
	case "off", "none", "no", "false", "disabled":
		return "off"
	case "on", "yes", "true", "enabled":
		return "zstd"
	default:
		// allow "zstd", "zstd:1..15", "lzo", "zlib", etc.
		return v
	}
}
