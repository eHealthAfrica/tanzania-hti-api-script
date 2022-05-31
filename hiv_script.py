from utils import (
    combine_hiv_data,
    export_datalink_telehub_hiv,
    export_hiv_gds,
    get_form_submissions,
    get_unique_patients,
)

# Form constants
HCW_REG_FORM = "0_hcw_registration"
PATIENT_REG_FORM = "HIV_1_patient_registration"
PATIENT_VISIT_FORM = "HIV_2_patient_visit"
HVL_DOC_FORM = "HIV_3_documentation_HVL_result"
HVL_INT_FORM = "HIV_4_interpretation_of_HVL_test_result"

# Output files
GDS_JSON_OUTFILE = "./hiv/google_data_studio.json"
TELEHUB_JSON_OUTFILE = "./hiv/telehub.json"

# Step 1: Get all submissions
# hcw_registrations = get_form_submissions(HCW_REG_FORM)
patients = get_form_submissions(PATIENT_REG_FORM)
unique_patients = get_unique_patients(patients)
visits = get_form_submissions(PATIENT_VISIT_FORM)
docs = get_form_submissions(HVL_DOC_FORM)
ints = get_form_submissions(HVL_INT_FORM)

# Step 2: Combine submissions
combined_data = combine_hiv_data(unique_patients, visits, docs, ints)

# Step 3: Export data for HIV Google Data Studio Endpoint
export_hiv_gds(combined_data, GDS_JSON_OUTFILE)

# Step 4: Export datalink Telehub HIV
export_datalink_telehub_hiv(combined_data, TELEHUB_JSON_OUTFILE)
