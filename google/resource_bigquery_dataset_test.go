package google

import (
	"fmt"
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

	datasetID := fmt.Sprintf("tf_test_%s", acctest.RandString(10))
	email := fmt.Sprintf("tf_test_%s@example.com", acctest.RandString(10))
	updated_email := fmt.Sprintf("tf_test_%s@example.com", acctest.RandString(10))

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckBigQueryDatasetDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccBigQueryDatasetWithAccess(datasetID, email),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckBigQueryDatasetExistsWithAccess(
						"google_bigquery_dataset.test", email),
				),
			},
			{
				Config: testAccBigQueryDatasetWithAccess(datasetID, updated_email),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckBigQueryDatasetExistsWithAccess(
						"google_bigquery_dataset.test", updated_email),
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

func testAccBigQueryDataset(datasetID string) string {
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "test" {
  dataset_id                  = "%s"
  friendly_name               = "foo"
  description                 = "This is a foo description"
  location                    = "EU"
  default_table_expiration_ms = 3600000

  labels {
    env                         = "foo"
    default_table_expiration_ms = 3600000
  }
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

  labels {
    env                         = "bar"
    default_table_expiration_ms = 7200000
  }
}`, datasetID)
}

func testAccBigQueryDatasetWithAccess(datasetID string, email string) string {
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "test" {
  dataset_id                  = "%s"
  friendly_name               = "baz"
  description                 = "This is a baz description"
  location                    = "EU"
  default_table_expiration_ms = 7200000

  labels {
    env                         = "baz"
    default_table_expiration_ms = 7200000
  }

  access [
	{
		role 		  = "READER"
		user_by_email = "%s"
	}
  ]
}`, datasetID, email)
}
