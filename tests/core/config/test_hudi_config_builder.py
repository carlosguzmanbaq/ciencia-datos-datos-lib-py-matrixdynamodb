import pytest


from dynamodb_curated_library.core.config.constants import Constants
from dynamodb_curated_library.core.config.hudi.hudi_config_builder import HudiConfigBuilder


class TestHudiConfigBuilder:
    """Test suite for HudiConfigBuilder class."""

    @pytest.fixture
    def basic_constants(self):
        """Basic constants without partition field."""
        return Constants(
            domain="productos",
            subdomain="transacciones",
            prefix_name="dynamodb",
            data_product="test_table",
            country_mesh="co",
            primary_key="order_id",
            package_name = "test_package",
            metadata_module = "metadata",
            precombine_key="job_process_date",
            insert_mode="append"
        )

    @pytest.fixture
    def constants_with_partition(self):
        """Constants with partition field."""
        return Constants(
            domain="productos",
            subdomain="transacciones",
            prefix_name="dynamodb",
            data_product="test_table",
            country_mesh="co",
            primary_key=["order_id", "user_id"],
            package_name = "test_package",
            metadata_module = "metadata",
            partition_field="year,month,day",
            precombine_key="update_date",
            insert_mode="overwrite"
        )

    def test_init_basic(self, basic_constants):
        """Test basic initialization."""
        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db"
        )

        assert builder.constants == basic_constants
        assert builder.table_name == "test_table"
        assert builder.curated_database == "test_db"
        assert builder.custom_options == {}

    def test_init_with_custom_options(self, basic_constants):
        """Test initialization with custom options."""
        custom_options = {"hoodie.metadata.enable": "true"}

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options=custom_options
        )

        assert builder.custom_options == custom_options

    def test_init_with_none_custom_options(self, basic_constants):
        """Test initialization with None custom options."""
        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options=None
        )

        assert builder.custom_options == {}

    def test_build_basic_config(self, basic_constants):
        """Test building basic Hudi configuration."""
        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = builder.build()

        # Verify base configuration
        expected_config = {
            "hoodie.table.name": "test_table",
            "hoodie.datasource.write.recordkey.field": "order_id",
            "hoodie.datasource.write.precombine.field": "job_process_date",
            "hoodie.metadata.enable": "false",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": "test_db",
            "hoodie.datasource.hive_sync.table": "test_table",
            "hoodie.datasource.hive_sync.support_timestamp": "true",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.mode": "hms",
            "mode": "append",
        }

        for key, value in expected_config.items():
            assert config[key] == value

    def test_build_with_partition_config(self, constants_with_partition):
        """Test building configuration with partition fields."""
        builder = HudiConfigBuilder(
            constants=constants_with_partition,
            table_name="partitioned_table",
            curated_database="test_db"
        )

        config = builder.build()

        # Verify partition configuration is added
        assert config["hoodie.datasource.hive_sync.partition_fields"] == "year,month,day"
        assert config["hoodie.datasource.write.partitionpath.field"] == "year,month,day"

        # Verify other fields
        assert config["hoodie.datasource.write.recordkey.field"] == "order_id,user_id"
        assert config["hoodie.datasource.write.precombine.field"] == "update_date"
        assert config["mode"] == "overwrite"

    def test_build_without_partition_field(self, basic_constants):
        """Test building configuration without partition field."""
        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = builder.build()

        # Verify partition fields are not present
        assert "hoodie.datasource.hive_sync.partition_fields" not in config
        assert "hoodie.datasource.write.partitionpath.field" not in config

    def test_build_with_empty_partition_field(self, basic_constants):
        """Test building configuration with empty partition field."""
        # Set empty partition field
        basic_constants.partition_field = ""

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = builder.build()

        # Verify partition fields are not added
        assert "hoodie.datasource.hive_sync.partition_fields" not in config
        assert "hoodie.datasource.write.partitionpath.field" not in config

    def test_build_with_custom_options_override(self, basic_constants):
        """Test building configuration with custom options that override defaults."""
        custom_options = {
            "hoodie.metadata.enable": "true",
            "hoodie.datasource.hive_sync.mode": "jdbc",
            "custom.option": "custom_value"
        }

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options=custom_options
        )

        config = builder.build()

        # Verify overrides
        assert config["hoodie.metadata.enable"] == "true"
        assert config["hoodie.datasource.hive_sync.mode"] == "jdbc"
        assert config["custom.option"] == "custom_value"

        # Verify other defaults remain
        assert config["hoodie.datasource.hive_sync.enable"] == "true"

    def test_build_with_custom_options_removal(self, basic_constants):
        """Test building configuration with custom options that remove defaults."""
        custom_options = {
            "hoodie.metadata.enable": None,
            "hoodie.datasource.hive_sync.support_timestamp": None
        }

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options=custom_options
        )

        config = builder.build()

        # Verify removed options
        assert "hoodie.metadata.enable" not in config
        assert "hoodie.datasource.hive_sync.support_timestamp" not in config

        # Verify other defaults remain
        assert config["hoodie.datasource.hive_sync.enable"] == "true"

    def test_build_with_mixed_custom_options(self, basic_constants):
        """Test building configuration with mixed custom options (override, add, remove)."""
        custom_options = {
            "hoodie.metadata.enable": "true",  # Override
            "custom.new.option": "new_value",  # Add
            "hoodie.datasource.hive_sync.use_jdbc": None  # Remove
        }

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options=custom_options
        )

        config = builder.build()

        # Verify override
        assert config["hoodie.metadata.enable"] == "true"

        # Verify addition
        assert config["custom.new.option"] == "new_value"

        # Verify removal
        assert "hoodie.datasource.hive_sync.use_jdbc" not in config

        # Verify other defaults remain
        assert config["hoodie.datasource.hive_sync.enable"] == "true"

    def test_add_partition_config_with_hasattr_false(self, basic_constants):
        """Test _add_partition_config when constants doesn't have partition_field attribute."""
        # Remove partition_field attribute if it exists
        if hasattr(basic_constants, 'partition_field'):
            delattr(basic_constants, 'partition_field')

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = {}
        builder._add_partition_config(config)

        # Verify no partition config is added
        assert "hoodie.datasource.hive_sync.partition_fields" not in config
        assert "hoodie.datasource.write.partitionpath.field" not in config

    def test_add_partition_config_with_none_partition_field(self, basic_constants):
        """Test _add_partition_config when partition_field is None."""
        basic_constants.partition_field = None

        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = {}
        builder._add_partition_config(config)

        # Verify no partition config is added
        assert "hoodie.datasource.hive_sync.partition_fields" not in config
        assert "hoodie.datasource.write.partitionpath.field" not in config

    def test_merge_custom_options_empty(self, basic_constants):
        """Test _merge_custom_options with empty custom options."""
        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options={}
        )

        config = {"existing.key": "existing_value"}
        original_config = config.copy()

        builder._merge_custom_options(config)

        # Verify config remains unchanged
        assert config == original_config

    def test_merge_custom_options_none(self, basic_constants):
        """Test _merge_custom_options with None custom options."""
        builder = HudiConfigBuilder(
            constants=basic_constants,
            table_name="test_table",
            curated_database="test_db",
            custom_options=None
        )

        config = {"existing.key": "existing_value"}
        original_config = config.copy()

        builder._merge_custom_options(config)

        # Verify config remains unchanged
        assert config == original_config

    def test_primary_key_normalization_string(self):
        """Test that string primary_key is properly handled."""
        constants = Constants(
            domain="test",
            subdomain="test",
            prefix_name="test",
            data_product="test",
            country_mesh="co",
            primary_key="single_key",  # String
            package_name = "test_package",
            metadata_module = "metadata"
        )

        builder = HudiConfigBuilder(
            constants=constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = builder.build()

        # Verify primary_key is normalized to string
        assert config["hoodie.datasource.write.recordkey.field"] == "single_key"

    def test_primary_key_normalization_list(self):
        """Test that list primary_key is properly handled."""
        constants = Constants(
            domain="test",
            subdomain="test",
            prefix_name="test",
            data_product="test",
            country_mesh="co",
            primary_key=["key1", "key2"],  # List
            package_name = "test_package",
            metadata_module = "metadata"
        )

        builder = HudiConfigBuilder(
            constants=constants,
            table_name="test_table",
            curated_database="test_db"
        )

        config = builder.build()

        # Verify primary_key is converted to comma-separated string
        assert config["hoodie.datasource.write.recordkey.field"] == "key1,key2"

    def test_build_integration_full_scenario(self):
        """Integration test with full scenario including all features."""
        constants = Constants(
            domain="productos",
            subdomain="transacciones",
            prefix_name="dynamodb",
            data_product="orders",
            country_mesh="co",
            primary_key=["order_id", "customer_id"],
            partition_field="year,month,day",
            precombine_key="update_timestamp",
            insert_mode="upsert",
            package_name = "test_package",
            metadata_module = "metadata"
        )

        custom_options = {
            "hoodie.metadata.enable": "true",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.hive_sync.use_jdbc": None,  # Remove
            "custom.business.rule": "enabled"
        }

        builder = HudiConfigBuilder(
            constants=constants,
            table_name="co_dynamodb_orders",
            curated_database="co_delfos_productos_curated_pdn_rl",
            custom_options=custom_options
        )

        config = builder.build()

        # Verify all components work together
        assert config["hoodie.table.name"] == "co_dynamodb_orders"
        assert config["hoodie.datasource.write.recordkey.field"] == "order_id,customer_id"
        assert config["hoodie.datasource.write.precombine.field"] == "update_timestamp"
        assert config["hoodie.datasource.hive_sync.database"] == "co_delfos_productos_curated_pdn_rl"
        assert config["hoodie.datasource.hive_sync.table"] == "co_dynamodb_orders"
        assert config["mode"] == "upsert"

        # Verify partition config
        assert config["hoodie.datasource.hive_sync.partition_fields"] == "year,month,day"
        assert config["hoodie.datasource.write.partitionpath.field"] == "year,month,day"

        # Verify custom options
        assert config["hoodie.metadata.enable"] == "true"  # Override
        assert config["hoodie.datasource.write.operation"] == "upsert"  # Add
        assert "hoodie.datasource.hive_sync.use_jdbc" not in config  # Remove
        assert config["custom.business.rule"] == "enabled"  # Add

        # Verify defaults remain
        assert config["hoodie.datasource.hive_sync.enable"] == "true"
        assert config["hoodie.datasource.hive_sync.mode"] == "hms"
