import time

from swchmonclient.thread_manager import MonitoringThreadManager


def counter_worker(stop_event, max_count=10000):
    """Count from 0 to max_count, sleeping 1 second per step, printing every 5 seconds."""
    for value in range(max_count + 1):
        if stop_event.is_set():
            print("Stop requested. Exiting worker.")
            return

        if value % 5 == 0:
            print(f"Counter: {value}")

        time.sleep(1)

    print("Counter finished.")


def main():
    manager = MonitoringThreadManager()
    thread_name = "counter-thread"

    manager.start_monitoring_thread(thread_name, counter_worker, max_count=10000)
    print("Thread started. Press Ctrl+C to stop early.")

    try:
        while manager.list_threads().get(thread_name, False):
            time.sleep(1)
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Stopping thread...")
    finally:
        if thread_name in manager.list_threads():
            manager.stop_monitoring_thread(thread_name, timeout=10.0)
        print("Thread handler test finished.")


if __name__ == "__main__":
    main()
