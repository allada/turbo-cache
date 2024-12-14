import { defineStore } from 'pinia'
import { ref } from 'vue'

// Define the Pinia store
export const useWebSocketStore = defineStore('ws', () => {
    const connect = () => {
        const webSocket = new WebSocket('ws://localhost:888');
        // Listen for open events
        webSocket.addEventListener("open", (event) => {
            console.log("Connected to WebSocket:", event);
        });
        // Listen for message events
        webSocket.addEventListener("message", (event) => {
            console.log("Message from server ", event.data);
            // TODO: Send the data to the data store
        });
        // Listen for close event
        webSocket.addEventListener("close", (event) => {
            console.log("Closed: ", event);
        });
        // Listen for errors events
        webSocket.addEventListener("error", (err) => {
            console.log("Error: ", err);
        });

      };
})
