# SD_Pract2
 
This project is a distributed implementation of a simple algorithm to ensure mutual exclusion.
It's implemented using IBM-PyWren and it consists of a number of slaves that want to modify a file named 'results.txt', the master will grant permission to the slaves.
You can change the number of slaves with the variable N_SLAVES. 
The order of execution of the slaves will be determined by the master, depending on the creation date of a file that every slave creates at their start.

The master returns the list of the slaves, the list will contain the order which he has given permission to the slaves and the main will check if the reuslt of the 'result.txt' file contains the same order than the list of the master.