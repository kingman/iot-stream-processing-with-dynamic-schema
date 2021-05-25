  
provider "google" {}

resource "google_project_service" "cloud-iot-apis" {
  project = var.google_project_id
  service = "cloudiot.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

resource "google_project_service" "pubsub-apis" {
  project = var.google_project_id
  service = "pubsub.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}

resource "google_project_service" "dataflow-apis" {
  project = var.google_project_id
  service = "dataflow.googleapis.com"

  disable_dependent_services = true
  disable_on_destroy         = true
}


resource "google_pubsub_topic" "default-telemetry" {
  name    = "default-telemetry"
  project = var.google_project_id

  depends_on = [
    google_project_service.pubsub-apis
  ]
}

resource "google_cloudiot_registry" "device-registry" {
  name    = var.google_iot_registry_id
  project = var.google_project_id
  region = var.google_default_region

  depends_on = [
    google_project_service.cloud-iot-apis,
    google_project_service.pubsub-apis,
    google_pubsub_topic.default-telemetry
  ]

  event_notification_configs {
    pubsub_topic_name = google_pubsub_topic.default-telemetry.id
  }

  http_config = {
    http_enabled_state = "HTTP_ENABLED"
  }

  mqtt_config = {
    mqtt_enabled_state = "MQTT_ENABLED"
  }
}

resource "google_cloudiot_device" "iot-device" {
  name     = var.google_iot_device_id
  registry = google_cloudiot_registry.device-registry.id

  metadata = {
    destination-dataset-unknown-message=var.google_bigquery_dataset_id
    destination-table-unknown-message=var.google_bigquery_unknown_message_table_id
    table-schema-unknown-message = file("../scripts/unknown-message-table-schema.json")

    input-data-schemas = file("../scripts/input-data-schema.json")

    destination-dataset-edgex=var.google_bigquery_dataset_id
    destination-table-edgex=var.google_bigquery_metrics_table_id
    table-schema-edgex = file("../scripts/edgex-table-schema.json")
    schema-map-edgex = file("../scripts/edgex-schema-mapping.json")
  }

  depends_on = [
      google_cloudiot_registry.device-registry
  ]
}

resource "google_bigquery_dataset" "dataset" {
    dataset_id = var.google_bigquery_dataset_id
    location = var.google_bigquery_default_region
    project = var.google_project_id
}

resource "google_dataflow_job" "streaming-processing" {
    name = "iot-event-processor"
    template_gcs_path = "gs://${var.google_dataflow_default_bucket}/templates/iot-stream-processing"
    temp_gcs_location = "gs://${var.google_dataflow_default_bucket}/tmp_dir"
    project = var.google_project_id
    region = var.google_default_region
    zone = var.google_default_zone
    machine_type = "n1-standard-1"

    parameters = {
        streaming = true
        numWorkers = 1
        inputTopic = google_pubsub_topic.default-telemetry.id
    }
    
    depends_on = [
        google_pubsub_topic.default-telemetry,
        google_project_service.dataflow-apis,
        google_bigquery_dataset.dataset
    ]
}
