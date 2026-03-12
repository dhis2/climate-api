from pydantic import BaseModel

class Dhis2DataValueSetConfig(BaseModel):
    """Mapping from aggregate outputs to DHIS2 DataValueSet fields."""

    data_element_uid: str
    category_option_combo_uid: str = "HllvX50cXC0"
    attribute_option_combo_uid: str = "HllvX50cXC0"
    data_set_uid: str | None = None
    org_unit_property: str = "id"
    stored_by: str | None = None