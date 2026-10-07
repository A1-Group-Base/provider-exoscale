package kafkacontroller

import (
	"testing"

	exoscalesdk "github.com/exoscale/egoscale/v3"
	"github.com/stretchr/testify/assert"
)

func TestKafkaPrimaryUsername(t *testing.T) {
	tests := map[string]struct {
		given    *exoscalesdk.DBAASServiceKafka
		expected string
	}{
		"GivenPrimaryUser_ThenReturnPrimaryUsername": {
			given: &exoscalesdk.DBAASServiceKafka{
				Users: []exoscalesdk.DBAASServiceKafkaUsers{
					{Type: "normal", Username: "someuser"},
					{Type: "primary", Username: "avnadmin"},
				},
				ConnectionInfo: &exoscalesdk.DBAASServiceKafkaConnectionInfo{
					ConnectURI: "https://otheruser@kafka-x.aivencloud.com:21699",
				},
			},
			expected: "avnadmin",
		},
		"GivenNoUsers_ThenReturnConnectURIUsername": {
			given: &exoscalesdk.DBAASServiceKafka{
				ConnectionInfo: &exoscalesdk.DBAASServiceKafkaConnectionInfo{
					ConnectURI: "https://avnadmin@kafka-x.aivencloud.com:21699",
				},
			},
			expected: "avnadmin",
		},
		"GivenOnlyNonPrimaryUsers_ThenReturnConnectURIUsername": {
			given: &exoscalesdk.DBAASServiceKafka{
				Users: []exoscalesdk.DBAASServiceKafkaUsers{
					{Type: "normal", Username: "someuser"},
				},
				ConnectionInfo: &exoscalesdk.DBAASServiceKafkaConnectionInfo{
					ConnectURI: "https://avnadmin@kafka-x.aivencloud.com:21699",
				},
			},
			expected: "avnadmin",
		},
		"GivenNilConnectionInfoAndNoUsers_ThenReturnEmpty": {
			given:    &exoscalesdk.DBAASServiceKafka{},
			expected: "",
		},
		"GivenConnectURIWithoutUserinfo_ThenReturnEmpty": {
			given: &exoscalesdk.DBAASServiceKafka{
				ConnectionInfo: &exoscalesdk.DBAASServiceKafkaConnectionInfo{
					ConnectURI: "https://kafka-x.aivencloud.com:21699",
				},
			},
			expected: "",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, kafkaPrimaryUsername(tc.given))
		})
	}
}

func TestGetConnectionDetails(t *testing.T) {
	given := &exoscalesdk.DBAASServiceKafka{
		URI: "kafka-x.aivencloud.com:21701",
		ConnectionInfo: &exoscalesdk.DBAASServiceKafkaConnectionInfo{
			AccessCert: "CERT",
			AccessKey:  "KEY",
		},
	}

	details, err := getConnectionDetails(given, "CA")
	assert.NoError(t, err)
	assert.Equal(t, []byte("CERT"), details["service.cert"])
	assert.Equal(t, []byte("KEY"), details["service.key"])
}
