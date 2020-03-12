# Persistent and Asynchronous Multicast System 
We implemented a system for supporting persistent asynchronous multicast on top of TCP. This is based on a coordinator-participant paradigm that includes both the multicast coordinator and the participants.

## Description
In this model, if a participant is offline more than some specified time threshold (say td), upon reconnecting, it will receive messages that were sent in the past td seconds. In other words, the messages sent before td will be permanently lost to the participant. If a participant is disconnected less than or equal to td seconds, it should not lose any messages.  
The participants send and receive multicast messages to and from the coordinator (i.e., participants do not directly communicate with each other). The coordinator manages the multicast group, handles all communication and stores messages for persistence.
