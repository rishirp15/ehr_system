import threading
import requests
import time
import uuid

API_URL = "http://localhost:5000"

PATIENT_A_UUID = str(uuid.uuid4()).split('-')[0]
PATIENT_B_UUID = str(uuid.uuid4()).split('-')[0]

def doctor_one_workflow():
    print("Doctor 1: Starting workflow...")
    payload_a = { "patient_uuid": PATIENT_A_UUID, "doctor_name": "Dr. Alice", "description": "Checkup for Patient A", "prescription": "None" }
    print("Doctor 1: Attempting to add record for Patient A...")
    try:
        requests.post(f"{API_URL}/record", json=payload_a, timeout=25)
        print("Doctor 1: ✅ Successfully added record for Patient A.")
    except requests.exceptions.Timeout:
        print("Doctor 1: ❌ Timed out adding record for Patient A."); return

    print("Doctor 1: Waiting 2 seconds..."); time.sleep(2)
    payload_b = { "patient_uuid": PATIENT_B_UUID, "doctor_name": "Dr. Alice", "description": "Follow-up for Patient B", "prescription": "Vitamins" }
    print("Doctor 1: Attempting to add record for Patient B...")
    try:
        requests.post(f"{API_URL}/record", json=payload_b, timeout=25)
        print("Doctor 1: ✅ Successfully added record for Patient B.")
    except requests.exceptions.Timeout:
        print("Doctor 1: ❌ Timed out adding record for Patient B. DEADLOCK OCCURRED AND RESOLVED.")

def doctor_two_workflow():
    print("Doctor 2: Starting workflow.")
    payload_b = { "patient_uuid": PATIENT_B_UUID, "doctor_name": "Dr. Bob", "description": "Checkup for Patient B", "prescription": "Rest" }
    print("Doctor 2: Attempting to add record for Patient B...")
    try:
        requests.post(f"{API_URL}/record", json=payload_b, timeout=25)
        print("Doctor 2: ✅ Successfully added record for Patient B.")
    except requests.exceptions.Timeout:
        print("Doctor 2: ❌ Timed out adding record for Patient B."); return

    print("Doctor 2: Waiting 2 seconds..."); time.sleep(2)
    payload_a = { "patient_uuid": PATIENT_A_UUID, "doctor_name": "Dr. Bob", "description": "Follow-up for Patient A", "prescription": "Further tests" }
    print("Doctor 2: Attempting to add record for Patient A...")
    try:
        requests.post(f"{API_URL}/record", json=payload_a, timeout=25)
        print("Doctor 2: ✅ Successfully added record for Patient A.")
    except requests.exceptions.Timeout:
        print("Doctor 2: ❌ Timed out adding record for Patient A. DEADLOCK OCCURRED AND RESOLVED.")

if __name__ == "__main__":
    print("--- Starting Deadlock Simulation ---")
    print(f"Patient A ID: {PATIENT_A_UUID}"); print(f"Patient B ID: {PATIENT_B_UUID}\n")
    thread1 = threading.Thread(target=doctor_one_workflow)
    thread2 = threading.Thread(target=doctor_two_workflow)
    thread1.start(); time.sleep(0.1); thread2.start()
    thread1.join(); thread2.join()
    print("\n--- Deadlock Simulation Finished ---")