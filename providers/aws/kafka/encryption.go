package kafka

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/stackshy/cloudemu/v2/internal/idgen"
)

// encryptionInfoKey is the RawOptions key holding a provisioned cluster's
// EncryptionInfo block (the wire layer stores the request block under it).
const encryptionInfoKey = "encryptionInfo"

// managedKafkaKeyARN returns the ARN of this account+region's AWS managed key
// for MSK (alias/aws/kafka). Real MSK encrypts data at rest under that key when
// a create names none, and DescribeCluster reports its key ARN in
// encryptionAtRest.dataVolumeKMSKeyId
// (https://docs.aws.amazon.com/msk/latest/developerguide/msk-working-with-encryption.html).
// There is one such key per account and region, so the key id is a UUID derived
// deterministically from both: every cluster in the account+region reports the
// same key, as in AWS.
func (m *Mock) managedKafkaKeyARN() string {
	sum := sha256.Sum256([]byte("alias/aws/kafka|" + m.opts.AccountID + "|" + m.opts.Region))
	// Shape the digest as a version-4 UUID, like a real KMS key id.
	sum[6] = sum[6]&0x0f | 0x40
	sum[8] = sum[8]&0x3f | 0x80
	h := hex.EncodeToString(sum[:16])
	keyID := h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]

	return idgen.AWSARN("kms", m.opts.Region, m.opts.AccountID, "key/"+keyID)
}

// withDefaultEncryption returns raw with the cluster's EncryptionInfo completed
// the way real MSK completes it: with no encryptionInfo, at-rest encryption
// under the AWS managed key plus the documented in-transit defaults (ClientBroker
// TLS, InCluster true); with an encryptionInfo that omits encryptionAtRest (or
// its key), the managed key is filled in and the rest is kept verbatim. A block
// that already names a key is left untouched. raw is not mutated.
func (m *Mock) withDefaultEncryption(raw map[string]json.RawMessage) map[string]json.RawMessage {
	out := copyRaw(raw)
	if out == nil {
		out = map[string]json.RawMessage{}
	}

	keyARN := m.managedKafkaKeyARN()

	blk, ok := out[encryptionInfoKey]
	if !ok || len(blk) == 0 {
		out[encryptionInfoKey] = mustJSON(map[string]any{
			"encryptionAtRest":    map[string]any{"dataVolumeKMSKeyId": keyARN},
			"encryptionInTransit": map[string]any{"clientBroker": "TLS", "inCluster": true},
		})

		return out
	}

	var info map[string]json.RawMessage
	if err := json.Unmarshal(blk, &info); err != nil || info == nil {
		return out
	}

	var atRest map[string]json.RawMessage
	if r, has := info["encryptionAtRest"]; has {
		_ = json.Unmarshal(r, &atRest)
	}

	if atRest == nil {
		atRest = map[string]json.RawMessage{}
	}

	var existing string
	if k, has := atRest["dataVolumeKMSKeyId"]; has {
		_ = json.Unmarshal(k, &existing)
	}

	if existing != "" {
		return out
	}

	atRest["dataVolumeKMSKeyId"] = mustJSON(keyARN)
	info["encryptionAtRest"] = mustJSON(atRest)
	out[encryptionInfoKey] = mustJSON(info)

	return out
}

// mustJSON marshals a value built from JSON-safe types (maps, strings, bools,
// raw messages), which cannot fail.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}

	return b
}
