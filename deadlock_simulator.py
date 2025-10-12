import threading
import requests
import time
import uuid

API_URL = "http://localhost:5000"

# Assume two patients have already been created and we have their UUIDs
# In a real test, you would first create these patients.
# For now, we generate random ones. You can replace them with real ones from the UI.
PATIENT_A_UUID = str(uuid.uuid4())
PATIENT_B_UUID = str(uuid.uuid4())

def doctor_one_workflow():
    """Doctor 1: Tries to lock A, then B."""
    print("Doctor 1: Starting workflow.")
    
    # Create a record for patient A (acquires lock on the data node)
    payload_a = {
        "patient_uuid": PATIENT_A_UUID, "doctor_name": "Alice",
        "description": "Checkup A", "prescription": "None"
    }
    print("Doctor 1: Attempting to add record for Patient A...")
    try:
        # A long timeout is needed to allow Maekawa's algorithm to engage
        requests.post(f"{API_URL}/record", json=payload_a, timeout=25)
        print("Doctor 1: ✅ Successfully added record for Patient A.")
    except requests.exceptions.Timeout:
        print("Doctor 1: ❌ Timed out adding record for Patient A. Deadlock likely.")
        return

    print("Doctor 1: Waiting for 2 seconds...")
    time.sleep(2)

    # Now, try to create a record for patient B
    payload_b = {
        "patient_uuid": PATIENT_B_UUID, "doctor_name": "Alice",
        "description": "Follow-up B", "prescription": "Vitamins"
    }
    print("Doctor 1: Attempting to add record for Patient B...")
    try:
        requests.post(f"{API_URL}/record", json=payload_b, timeout=25)
        print("Doctor 1: ✅ Successfully added record for Patient B.")
    except requests.exceptions.Timeout:
        print("Doctor 1: ❌ Timed out adding record for Patient B. Deadlock likely.")

def doctor_two_workflow():
    """Doctor 2: Tries to lock B, then A."""
    print("Doctor 2: Starting workflow.")

    # Create a record for patient B (acquires lock on the data node)
    payload_b = {
        "patient_uuid": PATIENT_B_UUID, "doctor_name": "Bob",
        "description": "Checkup B", "prescription": "Rest"
    }
    print("Doctor 2: Attempting to add record for Patient B...")
    try:
        requests.post(f"{API_URL}/record", json=payload_b, timeout=25)
        print("Doctor 2: ✅ Successfully added record for Patient B.")
    except requests.exceptions.Timeout:
        print("Doctor 2: ❌ Timed out adding record for Patient B. Deadlock likely.")
        return

    print("Doctor 2: Waiting for 2 seconds...")
    time.sleep(2)

    # Now, try to create a record for patient A
    payload_a = {
        "patient_uuid": PATIENT_A_UUID, "doctor_name": "Bob",
        "description": "Follow-up A", "prescription": "More tests"
    }
    print("Doctor 2: Attempting to add record for Patient A...")
    try:
        requests.post(f"{API_URL}/record", json=payload_a, timeout=25)
        print("Doctor 2: ✅ Successfully added record for Patient A.")
    except requests.exceptions.Timeout:
        print("Doctor 2: ❌ Timed out adding record for Patient A. Deadlock likely.")

if __name__ == "__main__":
    print("--- Starting Deadlock Simulation ---")
    print("This will simulate two doctors trying to update two patient records in a conflicting order.")
    print(f"Patient A UUID: {PATIENT_A_UUID}")
    print(f"Patient B UUID: {PATIENT_B_UUID}")
    
    thread1 = threading.Thread(target=doctor_one_workflow)
    thread2 = threading.Thread(target=doctor_two_workflow)

    thread1.start()
    # A tiny delay to increase the chance of deadlock
    time.sleep(0.1) 
    thread2.start()

    thread1.join()
    thread2.join()
    
    print("\n--- Deadlock Simulation Finished ---")
    print("Check the logs. If both doctors timed out on their second request, a deadlock was successfully simulated and resolved via timeout.")