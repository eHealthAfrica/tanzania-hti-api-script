import json
import math
from datetime import date, datetime

import pandas as pd
import requests
from decouple import config
from requests.auth import HTTPBasicAuth

AUTH_USER = config("AUTH_USER")
AUTH_PASSWORD = config("AUTH_PASSWORD")
BASE_URL = "https://eha-data.org/htidev/kernel/entities.json?ordering=-modified"


def make_request(url):
    r = requests.get(url, auth=HTTPBasicAuth(AUTH_USER, AUTH_PASSWORD))
    return r


def write_to_json(filename, my_arr):

    with open(filename, "w") as outfile:
        json.dump(my_arr, outfile)


def read_json_file(filename):
    f = open(
        filename,
    )
    return json.load(f)


def json_to_csv(input, output):
    df = pd.read_json(input)
    df.to_csv(output)


def get_payload(obj):
    payloads = []
    for item in obj:
        payloads.append(item["payload"])
    return payloads


def get_form_submissions(form_id):
    main_url = f"{BASE_URL}&page={1}&payload___id={form_id}"
    r = make_request(main_url)
    data = r.json()
    count = data["count"]
    pages = math.ceil(count / 10)
    submissions = []
    print("{} pages in total".format(pages))
    for x in range(1, pages + 1):
        print("Page {}".format(x))
        url = f"{BASE_URL}&page={x}&payload___id={form_id}"
        r = make_request(url)
        results = r.json()["results"]
        for r in results:
            submissions.append(r)
    payload = get_payload(submissions)
    print("{} submissions retrieved for {}".format(len(payload), form_id))
    print("___________________________________")
    return payload


def get_unique_patients(obj):
    count = len(obj)
    print("There were " + str(count) + " patients")
    # Remove duplicates
    duplicate_ids = []
    unique_ids = []
    for d in obj:
        patient_id = d["patient_id"]
        if patient_id not in unique_ids:
            unique_ids.append(patient_id)
        else:
            duplicate_ids.append(patient_id)
    duplicate_ids = list(set(duplicate_ids))
    unique_patients = []
    for d in obj:
        patient_id = d["patient_id"]
        if patient_id not in duplicate_ids:
            unique_patients.append(d)
    count = len(unique_patients)
    print("There were " + str(count) + " patients")
    return unique_patients


def harmonize_ids(obj):
    new_list = []
    for d in obj:
        d["patient_id"] = d["patient_id"].replace("ppTB", "psTB")
        new_list.append(d)
    return new_list


def age(birthdate):
    dob = datetime.strptime(birthdate[0:10], "%Y-%m-%d")
    today = date.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age


def get_tat(start, end):
    start_date = datetime.strptime(start[0:18], "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime(end[0:18], "%Y-%m-%dT%H:%M:%S")
    tat = end_date - start_date
    tat_in_days = tat.total_seconds() / 60 / 60 / 24
    if tat_in_days < 0:
        return None
    return tat_in_days


def combine_hiv_data(patients, visits, docs, ints):
    all_data = []
    for p in patients:
        # Add a visits list
        p["visits"] = []
        for v in visits:
            if p["patient_id"] == v["patient_id"]:
                p["visits"].append(v)
            for d in docs:
                if d["sample_id"] == v["sample_id"]:
                    v["documentation"] = d
            for i in ints:
                if i["sample_id"] == v["sample_id"]:
                    v["interpretation"] = i
        all_data.append(p)
    write_to_json("./data/hiv_data.json", all_data)
    print("Data combined")
    return all_data


def combine_tb_data(patients, docs, ints, visits):
    all_data = []
    for p in patients:
        sample = p["sputum_sample"]
        for d in docs:
            if d["sample_id"] == p["sample_id"]:
                sample["documentation"] = d

        for i in ints:
            if i["sample_id"] == p["sample_id"]:
                sample["interpretation"] = i

        p["visits"] = []
        for v in visits:
            if p["patient_id"] == v["patient_id"]:
                p["visits"].append(v)

        all_data.append(p)
    write_to_json("./data/tb_data.json", all_data)
    return all_data


def export_hiv_gds(data, outfile):

    num_hvl_tests = 0
    dump = []

    for d in data:

        obj = {}

        # Get date
        obj["date"] = d["end"]

        # Get place of registration
        obj["place_registration"] = d["place_registration"]

        # Get age
        dob = d["date_of_birth"]
        obj["age"] = age(dob)

        # Get gender
        obj["gender"] = d["gender"]

        visit_status = None
        hvl_result = None
        hvl_status = None
        scheduled_visit = 0
        tat_hiv_diagnostic = None
        tat_hiv_lab_diagnostic = None
        visit_counts = len(d["visits"])
        doc_count = 0
        int_count = 0

        if len(d["visits"]) > 0:

            for v in d["visits"]:

                # Get Scheduled visits
                if "appear_appointment" in v["ctc_patient"]:
                    if v["ctc_patient"]["appear_appointment"] == "Yes":
                        scheduled_visit = 1

                # Get HIV visit status
                visit_status = v["ctc_patient"]["visit"]

                if "documentation" in v.keys():
                    # Get Number of HIV viral load tests
                    num_hvl_tests += 1

                    doc_count += 1

                    hvl_result = v["documentation"]["test_group"]["test_result"]

                    if hvl_result < 1000:
                        hvl_status = "Suppressed"
                    else:
                        hvl_status = "Non-Suppressed"

                if "interpretation" in v.keys():
                    int_count += 1

            earliest_visit = d["visits"][-1]
            vis_date = earliest_visit["end"]

            # Get HIV diagnostic TAT
            if "interpretation" in earliest_visit.keys():
                int_date = earliest_visit["interpretation"]["end"]
                tat_hiv_diagnostic = get_tat(vis_date, int_date)

            # Get HIV lab diagnostic TAT
            if "documentation" in earliest_visit.keys():
                doc_date = earliest_visit["documentation"]["end"]
                tat_hiv_lab_diagnostic = get_tat(vis_date, doc_date)

        obj["visit_status"] = visit_status
        obj["hvl_result"] = hvl_result
        obj["hvl_status"] = hvl_status
        obj["scheduled_visit"] = scheduled_visit
        obj["tat_hiv_diagnostic"] = tat_hiv_diagnostic
        obj["tat_hiv_lab_diagnostic"] = tat_hiv_lab_diagnostic
        obj["visit_counts"] = visit_counts
        obj["doc_counts"] = doc_count
        obj["int_counts"] = int_count

        dump.append(obj)

    write_to_json(outfile, dump)
    print("There are " + str(num_hvl_tests) + " HVL tests")


def export_tb_gds(data, outfile):

    dump = []
    for d in data:
        obj = {}

        place_registration = d["place_registration"]
        obj["place_registration"] = place_registration

        reg_date = d["end"]
        obj["date"] = reg_date

        # Get age
        dob = d["tb_patient"]["date_of_birth"]
        obj["age"] = age(dob)

        # Get gender
        gender = d["tb_patient"]["gender"]
        obj["gender"] = gender

        # Get test type, test result, RMP_resistance
        test_type = ""
        test_result = ""
        tb_resistance = ""
        tat_hcw_msg_received = None
        documentation_date = None
        sample = d["sputum_sample"]
        if "documentation" in sample.keys():
            test_type = sample["documentation"]["sample"]["testing_method"]
            if test_type == "GeneXpert":
                test_result = sample["documentation"]["sample"]["GeneXpert_result"]
            else:
                test_result = sample["documentation"]["sample"]["AFB_result"]
            if "RMP_resistance" in sample["documentation"]["sample"]:
                tb_resistance = sample["documentation"]["sample"]["RMP_resistance"]
            documentation_date = sample["documentation"]["end"]
            tat_hcw_msg_received = get_tat(reg_date, documentation_date)

        obj["test_type"] = test_type
        obj["test_result"] = test_result
        obj["tb_resistance"] = tb_resistance

        # Get symptoms and related symptoms
        symptoms = d["tb_patient"]["symptoms"]
        symptoms_optional = d["tb_patient"]["symptoms_optional"]

        # symptoms
        cough_symptom = 0
        chest_pain_symptom = 0
        haemoptysis_symptom = 0
        dyspnoea_symptom = 0

        if "Cough" in symptoms:
            cough_symptom = 1
        if "Chest_pain" in symptoms:
            chest_pain_symptom = 1
        if "Haemoptysis" in symptoms:
            haemoptysis_symptom = 1
        if "Dyspnoea" in symptoms:
            dyspnoea_symptom = 1

        # Additional symptoms
        fever_ad_symptom = 0
        weight_ad_loss_symptom = 0
        loss_of_appetite_ad_symptom = 0
        night_sweats_ad_symptom = 0

        if "Fever" in symptoms_optional:
            fever_ad_symptom = 1
        if "Night_sweats" in symptoms_optional:
            weight_ad_loss_symptom = 1
        if "Loss_of_appetite" in symptoms_optional:
            loss_of_appetite_ad_symptom = 1
        if "Night_sweats" in symptoms_optional:
            night_sweats_ad_symptom = 1

        obj["cough_symptom"] = cough_symptom
        obj["chest_pain_symptom"] = chest_pain_symptom
        obj["haemoptysis_symptom"] = haemoptysis_symptom
        obj["dyspnoea_symptom"] = dyspnoea_symptom
        obj["fever_ad_symptom"] = fever_ad_symptom
        obj["weight_ad_loss_symptom"] = weight_ad_loss_symptom
        obj["loss_of_appetite_ad_symptom"] = loss_of_appetite_ad_symptom
        obj["night_sweats_ad_symptom"] = night_sweats_ad_symptom

        # Get tb contact
        tb_contact = d["tb_patient"]["tb_contact"]
        obj["tb_contact"] = tb_contact

        # Get TB Diagnostic tat
        tat_diagnostic = None

        interpretation_date = None
        if "interpretation" in sample.keys():
            interpretation_date = sample["interpretation"]["end"]
            tat_diagnostic = get_tat(reg_date, interpretation_date)

        obj["tat_diagnostic"] = tat_diagnostic

        # Get TB outcomes
        tb_outcome = ""
        visits = d["visits"]
        final_visit_date = None
        initial_visit_date = None
        tat_tb_treatment_completed = None
        tat_tb_treatment_initiated = None
        treatment_status = ""
        if len(visits) > 0:
            for v in visits:
                visit_type = v["type_of_visit"]
                if visit_type == "documentation_final_outcome":
                    tb_outcome = v["further_procedure"]["final_outcome"]
                elif (
                    visit_type == "initiation_treatment"
                    and "final_outcome" in v["further_procedure"]
                ):
                    tb_outcome = v["further_procedure"]["final_outcome"]
                else:
                    pass

                # Get TAT TB suspect until treatment initiated
                if (
                    "final_outcome" not in v["further_procedure"]
                    and visit_type == "initiation_treatment"
                ):
                    initial_visit_date = v["end"]
                    tat_tb_treatment_initiated = get_tat(reg_date, initial_visit_date)
                    treatment_status = "initiated"

                # Get TAT TB suspect until treatment completed
                if "final_outcome" in v["further_procedure"]:
                    final_visit_date = v["end"]
                    tat_tb_treatment_completed = get_tat(reg_date, final_visit_date)
                    treatment_status = "completed"

        obj["tb_outcome"] = tb_outcome
        obj["tat_tb_treatment_completed"] = tat_tb_treatment_completed
        obj["tat_tb_treatment_initiated"] = tat_tb_treatment_initiated
        obj["tat_hcw_msg_received"] = tat_hcw_msg_received

        obj["treatment_status"] = treatment_status

        time_diagnosis_to_treatment = None
        if initial_visit_date and documentation_date:
            time_diagnosis_to_treatment = get_tat(
                documentation_date, initial_visit_date
            )

        obj["time_diagnosis_to_treatment"] = time_diagnosis_to_treatment

        dump.append(obj)

    write_to_json(outfile, dump)


def export_datalink_telehub_hiv(data, outfile):

    dump = []

    for d in data:

        obj = {}

        hvl_result = None
        date = None

        if len(d["visits"]) > 0:
            last_visit = d["visits"][0]
            if "documentation" in last_visit.keys():
                hvl_result = last_visit["documentation"]["test_group"]["test_result"]
                date = last_visit["documentation"]["end"]

        # Get Identifier in HTI of patient
        obj["patient_id"] = d["patient_id"]
        # Get Viral load of HIV test
        obj["hvl_result"] = hvl_result
        # Get Date of HVL test
        obj["date"] = date

        dump.append(obj)

    write_to_json(outfile, dump)


def export_data_for_dhis2(data, outfile):

    dump = []
    for d in data:

        sample = d["sputum_sample"]
        if "documentation" in sample.keys():
            test_type = sample["documentation"]["sample"]["testing_method"]
            if test_type == "GeneXpert":
                test_result = sample["documentation"]["sample"]["GeneXpert_result"]
            else:
                test_result = sample["documentation"]["sample"]["AFB_result"]
            if test_result == "positive":

                obj = {}

                reg_date = d["end"]
                obj["reg_date"] = reg_date

                # Get age
                dob = d["tb_patient"]["date_of_birth"]
                obj["age"] = age(dob)

                # Get gender
                gender = d["tb_patient"]["gender"]
                obj["gender"] = gender

                obj["test_result"] = test_result

                place_registration = d["place_registration"]
                obj["place_registration"] = place_registration

                documentation_date = sample["documentation"]["end"]
                obj["positive_test_date"] = documentation_date

                dump.append(obj)

    write_to_json(outfile, dump)


def get_submission_meta(submissions):
    dump = []
    for s in submissions:
        obj = {}
        obj["form_id"] = s["id"]
        obj["form_name"] = s["_id"]
        obj["start_date"] = s["start"]
        obj["end_date"] = s["end"]
        obj["surveyor"] = s["_surveyor"]
        obj["version"] = s["_version"]
        obj["submitted_at"] = s["_submitted_at"]
        dump.append(obj)
    return dump


def export_sys_monitoring_data(patients, docs, ints, visits, outfile):
    # List of forms 1 submitted
    form1_list = get_submission_meta(patients)
    # List of forms 2 submitted
    form2_list = get_submission_meta(docs)
    # List of forms 3 submitted
    form3_list = get_submission_meta(ints)
    # List of forms 4 submitted
    form4_list = get_submission_meta(visits)
    all_forms = form1_list + form2_list + form3_list + form4_list

    write_to_json(outfile, all_forms)

    print(len(all_forms))
