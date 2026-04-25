import asyncio
import sys
import os
from comm_ipc.client import CommIPC
from comm_ipc.comm_data import CommData

async def main():
    socket_path = "/run/user/1000/comm_ipc/comm_default.sock"
    client = CommIPC(client_id="python-tester-final", verbose=True, socket_path=socket_path)
    
    print(f"Connecting to {socket_path}...")
    await client.connect()
    
    print("Opening channel 'rust-demo-channel'...")
    channel = await client.open("rust-demo-channel")

    # 1. Call Rust RPC
    print("\n[PYTHON TEST 1] Calling Rust RPC 'get_status'...")
    res = await channel.event("get_status", {})
    print(f"Result from Rust: {res}")

    # 2. Call Rust Group RPC
    print("\n[PYTHON TEST 2] Calling Rust Group RPC 'heavy-tasks.process'...")
    res = await channel.group("heavy-tasks").get("process", {"data": "python-call"})
    print(f"Result from Rust Group: {res}")

    # 3. Consume Rust Stream
    print("\n[PYTHON TEST 3] Consuming Rust Stream 'get_logs'...")
    async for chunk in channel.stream("get_logs", {}):
        print(f"Stream chunk from Rust: {chunk}")

    # 4. Subscribe to Rust Alerts
    print("\n[PYTHON TEST 4] Subscribing to Rust 'alerts'...")
    async def on_alert(cd: CommData):
        print(f"!!! PYTHON RECEIVED ALERT FROM RUST: {cd.data}")

    await channel.subscribe("alerts", on_alert)
    
    print("\nPython tester finished active calls. Waiting 5s for alerts...")
    try:
        await asyncio.sleep(5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
