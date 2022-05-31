from utils import (
    combine_tb_data,
    export_data_for_dhis2,
    export_sys_monitoring_data,
    export_tb_gds,
    get_form_submissions,
    get_unique_patients,
    harmonize_ids,
)

# Form constants
REG_FORM = "TB_1_suspect_registration"
DOC_FORM = "TB_2_documentation_tb_test_result"
INT_FORM = "TB_3_interpretation_tb_test_result"
VISIT_FORM = "TB_4_patient_visit_health facility"

# Output files
GDS_OUTFILE = "./tb/gds.json"
DHIS2_OUTFILE = "./tb/dhis2.json"
SYS_MONITORING_OUTFILE = "./tb/sys_metadata.json"

# Step 1: Get submissions
patients = harmonize_ids(get_unique_patients(get_form_submissions(REG_FORM)))
docs = get_form_submissions(DOC_FORM)
ints = get_form_submissions(INT_FORM)
visits = get_form_submissions(VISIT_FORM)

# Step 2: Combine TB Submissons
combined_data = combine_tb_data(patients, docs, ints, visits)

# Step 3: Export data for GDS
export_tb_gds(combined_data, GDS_OUTFILE)

# Step 4: Export data for DHIS2 data transfer
export_data_for_dhis2(combined_data, DHIS2_OUTFILE)

# Step 5: Export system monitoring data
export_sys_monitoring_data(patients, docs, ints, visits, SYS_MONITORING_OUTFILE)
