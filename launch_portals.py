import webbrowser
import time

PORTALS = {
    "Patient": "http://localhost:8080/patient_portal.html",
    "Doctor": "http://localhost:8080/doctor_portal.html",
    "Admin": "http://localhost:8080/admin_dashboard.html"
}
WAIT_SECONDS = 10 

if __name__ == "__main__":
    print(f"[*] Waiting {WAIT_SECONDS} seconds for the system to initialize...")
    time.sleep(WAIT_SECONDS)
    
    print("[*] Launching web portals...")
    
    for portal_name, url in PORTALS.items():
        try:
            print(f"    -> Opening {portal_name} Portal...")
            webbrowser.open_new_tab(url)
        except Exception as e:
            print(f"[ERROR] Could not open {url}. Please open it manually. Error: {e}")
            
    print("\n[SUCCESS] Startup script finished. Your MediSync Pro system is ready.")