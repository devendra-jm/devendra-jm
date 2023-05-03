import os
def camel_case(st):
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]

def pascal_case(st):
    out = camel_case(st)
    return out[0].upper() + out[1:]

#customer_user_id
query = """
                select distinct 
                    a.user_id as customer_user_id
                from dwh_onboarding.report_onboarding_started a
                left join dwh_user_demographics.dim_user_demographic b
                on a.user_id = b.user_id
                where 
                    b.age_on_savings_account_creation >= 22
                    and b.income_slab_updated in ('c.5L-10L', 'd.10L-15L', 'e.15L-25L', 'f.25L+') 
"""

name = "users age gte22 and salary gte5lk".replace(" ", "_").replace("_", " ")
product = "user_quality"

formated_name = f"{name.lower()} event publisher"
job_name = formated_name.replace(" ", "_")
event_name = job_name.replace("_", "-").replace("-publisher", "")
event_name_versioned = f"{event_name}-v1"
job_name_upper = formated_name.upper().replace(" ", "_")
job_name_camel = camel_case(formated_name)


def get_file_contents(path):
    text_file = open(path, "r")
    data = text_file.read()
    return data

def write_file_content(path, data):
    file = open(path, "w")
    file.write(data)
    file.close()



def generate_file(source_file_path, destination_path, macro_map):
    data = get_file_contents(source_file_path)
    for key in macro_map:
        data = data.replace(f"<<{key}>>", macro_map[key])
    write_file_content(destination_path, data)

macro = {
    "spark-job-name": job_name.replace("_", "-"),
    "arg-prefix": job_name_upper,
    "event-data-location": job_name_camel + "DataS3Location",
    "dag_name": f"{job_name}_job",
    "formated_name": f"{formated_name} job",
    "event_name": event_name,
    "event_name_versioned": event_name_versioned,
    "product": product,
    "user_list_query": query
}

print(macro)

out_path = f"/Users/devendraparhate/Desktop/temp/comms_job/out/{job_name}/"
os.mkdir(out_path)

template_spark_application_file = "/Users/devendraparhate/Desktop/temp/comms_job/template/spark_application.yaml"
generated_spark_application_file = f"{out_path}/spark_application.yaml"
generate_file(template_spark_application_file, generated_spark_application_file, macro)

template_python_file = "/Users/devendraparhate/Desktop/temp/comms_job/template/job_name.py"
generated_python_file = f"{out_path}/{job_name}.py"
generate_file(template_python_file, generated_python_file, macro)

template_spark_application_file = "/Users/devendraparhate/Desktop/temp/comms_job/template/constants.json"
generated_spark_application_file = f"{out_path}/constants.json"
generate_file(template_spark_application_file, generated_spark_application_file, macro)

generated_init_python_file = f"{out_path}/__init__.py"
generate_file(template_spark_application_file, generated_init_python_file, macro)

