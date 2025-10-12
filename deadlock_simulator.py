import threading
import requests
import time
import uuid

API_URL = "http://localhost:5000"

# We generate two random, unique patient UUIDs for the simulation.
# In a real scenario, you might get these from the patient roster.
PATIENT_A_UUID = str(uuid.uuid4())
PATIENT_B_UUID = str(uuid.uuid4())

def doctor_one_workflow():
    """
    Workflow for the first doctor.
    1. Adds a record for Patient A (acquiring a lock).
    2. Waits briefly.
    3. Tries to add a record for Patient B (and will wait for the lock held by Doctor Two).
    """
    print("Doctor 1: Starting workflow...")
    
    payload_a = {
        "patient_uuid": PATIENT_A_UUID, "doctor_name": "Dr. Alice",
        "description": "Initial checkup for Patient A", "prescription": "None"
    }
    print("Doctor 1: Attempting to add record for Patient A...")
    try:
        # The timeout is set high enough to allow the distributed lock to engage.
        requests.post(f"{API_URL}/record", json=payload_a, timeout=25)
        print("Doctor 1: ✅ Successfully added record for Patient A.")
    except requests.exceptions.Timeout:
        print("Doctor 1: ❌ Timed out adding record for Patient A. Deadlock likely.")
        return

    print("Doctor 1: Waiting for 2 seconds before next operation...")
    time.sleep(2)

    payload_b = {
        "patient_uuid": PATIENT_B_UUID, "doctor_name": "Dr. Alice",
        "description": "Follow-up for Patient B", "prescription": "Vitamins"
    }
    print("Doctor 1: Attempting to add record for Patient B...")
    try:
        requests.post(f"{API_URL}/record", json=payload_b, timeout=25)
        print("Doctor 1: ✅ Successfully added record for Patient B.")
    except requests.exceptions.Timeout:
        print("Doctor 1: ❌ Timed out adding record for Patient B. DEADLOCK OCCURRED AND RESOLVED.")

def doctor_two_workflow():
    """
    Workflow for the second doctor.
    1. Adds a record for Patient B (acquiring a lock).
    2. Waits briefly.
    3. Tries to add a record for Patient A (and will wait for the lock held by Doctor One).
    """
    print("Doctor 2: Starting workflow.")

    payload_b = {
        "patient_uuid": PATIENT_B_UUID, "doctor_name": "Dr. Bob",
        "description": "Initial checkup for Patient B", "prescription": "Rest"
    }
    print("Doctor 2: Attempting to add record for Patient B...")
    try:
        requests.post(f"{API_URL}/record", json=payload_b, timeout=25)
        print("Doctor 2: ✅ Successfully added record for Patient B.")
    except requests.exceptions.Timeout:
        print("Doctor 2: ❌ Timed out adding record for Patient B. Deadlock likely.")
        return

    print("Doctor 2: Waiting for 2 seconds before next operation...")
    time.sleep(2)

    payload_a = {
        "patient_uuid": PATIENT_A_UUID, "doctor_name": "Dr. Bob",
        "description": "Follow-up for Patient A", "prescription": "Further tests"
    }
    print("Doctor 2: Attempting to add record for Patient A...")
    try:
        requests.post(f"{API_URL}/record", json=payload_a, timeout=25)
        print("Doctor 2: ✅ Successfully added record for Patient A.")
    except requests.exceptions.Timeout:
        print("Doctor 2: ❌ Timed out adding record for Patient A. DEADLOCK OCCURRED AND RESOLVED.")

if __name__ == "__main__":
    print("--- Starting Deadlock Simulation ---")
    print("This will simulate two 'doctors' trying to update two patient records in a conflicting order.")
    print(f"Patient A UUID: {PATIENT_A_UUID}")
    print(f"Patient B UUID: {PATIENT_B_UUID}\n")
    
    thread1 = threading.Thread(target=doctor_one_workflow)
    thread2 = threading.Thread(target=doctor_two_workflow)

    # Start the two workflows concurrently
    thread1.start()
    time.sleep(0.1) # A tiny delay to increase the chance of the race condition
    thread2.start()

    # Wait for both threads to complete
    thread1.join()
    thread2.join()
    
    print("\n--- Deadlock Simulation Finished ---")
    print("Check the logs above. If both doctors timed out on their second request, a deadlock was successfully simulated and resolved via timeout.")