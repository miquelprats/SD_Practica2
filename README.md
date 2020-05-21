# SD_Pract2
 
This project is a distributed implementation of a simple algorithm to ensure mutual exclusion.
It's implemented using IBM-PyWren
It consists of a master that will grant permission to the slaves.
You can change the number of slaves with the variable N_SLAVES. 
The order of execution of the slaves will be determined by the master, depending on the creation date of a file that every slave creates at their start.