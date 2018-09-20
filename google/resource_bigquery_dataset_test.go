package google

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccBigQueryDataset_basic(t *testing.T) {
	t.Parallel()

	datasetID := fmt.Sprintf("tf_test_%s", acctest.RandString(10))

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckBigQueryDatasetDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccBigQueryDataset(datasetID),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckBigQueryDatasetExists(
						"google_bigquery_dataset.test"),
				),
			},

			{
				Config: testAccBigQueryDatasetUpdated(datasetID),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckBigQueryDatasetExists(
						"google_bigquery_dataset.test"),
				),
			},
		},
	})
}

func TestAccBigQueryDataset_withAccess(t *testing.T) {
	t.Parallel()

	datasetId := fmt.Sprintf("tf_test_%s", acctest.RandString(10))

	email := fmt.Sprintf("tf_test_%s@example.com", acctest.RandString(10))
	updatedEmail := fmt.Sprintf("tf_test_%s@example.com", acctest.RandString(10))

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckBigQueryDatasetDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccBigQueryDatasetWithAccess(datasetId, email),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckBigQueryDatasetExistsWithAccess(
						"google_bigquery_dataset.test", email),
				),
			},
			{
				Config: testAccBigQueryDatasetWithAccess(datasetId, updatedEmail),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckBigQueryDatasetExistsWithAccess(
						"google_bigquery_dataset.test", updatedEmail),
				),
			},
		},
	})
}

func testAccCheckBigQueryDatasetDestroy(s *terraform.State) error {
	config := testAccProvider.Meta().(*Config)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "google_bigquery_dataset" {
			continue
		}

		_, err := config.clientBigQuery.Datasets.Get(config.Project, rs.Primary.Attributes["dataset_id"]).Do()
		if err == nil {
			return fmt.Errorf("Dataset still exists")
		}
	}

	return nil
}

func testAccCheckBigQueryDatasetExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		config := testAccProvider.Meta().(*Config)

		found, err := config.clientBigQuery.Datasets.Get(config.Project, rs.Primary.Attributes["dataset_id"]).Do()
		if err != nil {
			return err
		}

		if found.Id != rs.Primary.ID {
			return fmt.Errorf("Dataset not found")
		}

		return nil
	}
}

func testAccCheckBigQueryDatasetExistsWithAccess(resourceId string, email string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceId]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceId)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		config := testAccProvider.Meta().(*Config)

		found, err := config.clientBigQuery.Datasets.Get(config.Project, rs.Primary.Attributes["dataset_id"]).Do()
		if err != nil {
			return err
		}

		if found.Id != rs.Primary.ID {
			return fmt.Errorf("Dataset not found")
		}

		if found.Access == nil {
			return fmt.Errorf("Access object missing on dataset")
		}

		access := found.Access[0]
		if access.UserByEmail != email {
			return fmt.Errorf("Value of Access.UserByEmail does not match expected value")
		}

		return nil
	}
}

func retrieveCurrentClientEmail() string {
	var account accountFile
	if v := os.Getenv("GOOGLE_CREDENTIALS"); v != "" {
		err := json.Unmarshal([]byte(v), &account)
		if err != nil {
			// What?
		}

		return account.ClientEmail
	} else {
		// What here?
		panic(errors.New("GOOGLE CREDENTIALS MISSING?"))
	}
}

func testAccBigQueryDataset(datasetID string) string {
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "test" {
  dataset_id                  = "%s"
  friendly_name               = "foo"
  description                 = "This is a foo description"
  location                    = "EU"
  default_table_expiration_ms = 3600000

  labels = {
    env                         = "foo"
    default_table_expiration_ms = 3600000
  }

  access = [
	{
		role 		  = "OWNER"
		user_by_email = "example@example.com"
	}
  ]
}`, datasetID)
}

func testAccBigQueryDatasetUpdated(datasetID string) string {
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "test" {
  dataset_id                  = "%s"
  friendly_name               = "bar"
  description                 = "This is a bar description"
  location                    = "EU"
  default_table_expiration_ms = 7200000

  labels = {
    env                         = "bar"
    default_table_expiration_ms = 7200000
  }

  access = [
	{
		role 		  = "OWNER"
		user_by_email = "example@example.com"
	}
  ]
}`, datasetID)
}

func testAccBigQueryDatasetWithAccess(datasetId string, email string) string {
	projectId := getTestProjectFromEnv()

	viewDatasetId := fmt.Sprintf("tf_test_view_dataset_%s", acctest.RandString(10))
	viewTableId := fmt.Sprintf("tf_test_view_%s", acctest.RandString(10))
	testServiceAccountId := fmt.Sprintf("tf-test-%s", acctest.RandString(10))

	return fmt.Sprintf(`
resource "google_service_account" "test_account" {
  account_id   = "%s"
  display_name = "test service account"
  project      = "%s"
}

resource "google_bigquery_dataset" "test_view_dataset" {
  dataset_id = "%s"
  project = "%s"
  access = [
	{
		role 		  = "OWNER"
		user_by_email = "${google_service_account.test_account.email}"
	}
  ]
}

resource "google_bigquery_table" "test_view" {
  table_id   = "%s"
  dataset_id = "${google_bigquery_dataset.test_view_dataset.dataset_id}"
  project = "%s"

  time_partitioning {
    type = "DAY"
  }

  view {
  	query = "%s"
  	use_legacy_sql = false
  }
}  

resource "google_bigquery_dataset" "test_dataset" {
  dataset_id                  = "%s"
  friendly_name               = "baz"
  description                 = "This is a baz description"
  location                    = "EU"
  default_table_expiration_ms = 7200000

  labels = {
    env                         = "baz"
    default_table_expiration_ms = 7200000
  }

  access = [
	{
		role 		  = "OWNER"
		user_by_email = "example@example.com"
	},
	{
		role 		  = "READER"
		user_by_email = "${google_service_account.test_account.email}"
	},
	{
		view = {
			dataset_id = "${google_bigquery_dataset.test_view_dataset.dataset_id}"
			project_id = "%s"
			table_id   = "${google_bigquery_table.test_view.table_id}"
		}
	}
  ]
}`, testServiceAccountId, projectId, viewDatasetId, projectId, viewTableId, projectId,
		"SELECT state FROM `lookerdata:cdc.project_tycho_reports`", datasetId, projectId)
}
