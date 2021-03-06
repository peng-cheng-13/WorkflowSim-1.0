/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

/**
 * An implementation of a storage system. It simulates the behaviour of a typical harddrive storage.
 * 
 * @author Peng Cheng
 * @since CloudSim Toolkit 1.0
 */
public class HybridStorage {


        /** the first storage system*/
        private HarddriveStorage[] storageSystem = null;

	/**
	 * Creates a new hybrid storage.
	 * 
	 * @throws ParameterException when the name and the capacity are not valid
	 */
	public HybridStorage() throws ParameterException {
               
		storageSystem = new HarddriveStorage[3];
                /*Init In-memory file system, 620 GB*/
                storageSystem[0] = new HarddriveStorage("Ramdisk", 634880);
                storageSystem[0].setMaxTransferRate(1100);
                storageSystem[0].setLatency(0);
                storageSystem[0].setAvgSeekTime(0);

                /*Init Local file system, 1240 GB*/
                storageSystem[1] = new HarddriveStorage("LocalFS", 1269760);
                storageSystem[1].setMaxTransferRate(1000);
                storageSystem[1].setLatency(0.0001);
                storageSystem[1].setAvgSeekTime(0.0001);

                /*Init Parallel file system, 1240 GB*/
                storageSystem[2] = new HarddriveStorage("Lustre", 1269760);
                storageSystem[2].setMaxTransferRate(600);
                storageSystem[2].setLatency(0.1);
                storageSystem[2].setAvgSeekTime(0.1);
	}
	

	/**
	 * Gets the available space in MB.
	 * @param i the id of storage system
	 * @return the available space in MB
	 */
	public double getAvailableSpace(int i) {
		return storageSystem[i].getAvailableSpace();
	}

	/**
	 * Checks if the storage is full or not.
	 * @param i the id of storage system
	 * 
	 * @return <tt>true</tt> if the storage is full, <tt>false</tt> otherwise
	 */
	public boolean isFull(int i) {
		return storageSystem[i].isFull();
	}

	/**
	 * Gets the number of files stored on this storage.
	 * @param i the id of storage system
	 * 
	 * @return the number of stored files
	 */
	public int getNumStoredFile(int i) {
		return storageSystem[i].getNumStoredFile();
	}

	/**
	 * Makes a reservation of the space on the storage to store a file.
	 * 
	 * @param fileSize the size to be reserved in MB
	 * @param i the id of storage system
	 *
	 * @return <tt>true</tt> if reservation succeeded, <tt>false</tt> otherwise
	 */
	public boolean reserveSpace(int fileSize, int i) {
		return storageSystem[i].reserveSpace(fileSize);
	}

	/**
	 * Adds a file for which the space has already been reserved. The time taken (in seconds) for
	 * adding the file can also be found using {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file the file to be added
	 * @param i the id of storage system
	 *
	 * @return the time (in seconds) required to add the file
	 */
	public double addReservedFile(File file, int i) {
		return storageSystem[i].addReservedFile(file);
	}

	/**
	 * Checks whether there is enough space on the storage for a certain file.
	 * 
	 * @param fileSize a FileAttribute object to compare to
	 * @param i the id of storage system
	 * @return <tt>true</tt> if enough space available, <tt>false</tt> otherwise
	 */
	public boolean hasPotentialAvailableSpace(int fileSize, int i) {
		return storageSystem[i].hasPotentialAvailableSpace(fileSize);
	}

	/**
	 * Gets the total capacity of the storage in MB.
	 * @param i the id of storage system
	 * 
	 * @return the capacity of the storage in MB
	 */
	public double getCapacity(int i) {
		return storageSystem[i].getCapacity();
	}

	/**
	 * Gets the current size of the stored files in MB.
	 * @param i the id of storage system
	 * 
	 * @return the current size of the stored files in MB
	 */
	public double getCurrentSize(int i) {
		return storageSystem[i].getCurrentSize();
	}

	/**
	 * Gets the name of the storage.
	 * @param i the id of storage system
	 * 
	 * @return the name of this storage
	 */
	public String getName(int i) {
		return storageSystem[i].getName();
	}

	/**
	 * Gets the latency of this harddrive in seconds.
	 * @param i the id of storage system
	 * 
	 * @return the latency in seconds
	 */
	public double getLatency(int i) {
		return storageSystem[i].getLatency();
	}

	/**
	 * Gets the maximum transfer rate of the storage in MB/sec.
	 * @param i the id of storage system
	 * 
	 * @return the maximum transfer rate in MB/sec
	 */
	public double getMaxTransferRate(int i) {
		return storageSystem[i].getMaxTransferRate();
	}

	/**
	 * Gets the average seek time of the harddrive in seconds.
	 * @param i the id of storage system
	 * 
	 * @return the average seek time in seconds
	 */
	public double getAvgSeekTime(int i) {
		return storageSystem[i].getAvgSeekTime();
	}

        /**
         * Locate the id of storage system that contains required file.
         * @param fileName the name of the file we are looking for
         *
         * @return the id of storage system
         */
	public int locateFile(String fileName) {
          int rvalue = -1;
          for (int i = 0; i < 3; i++) {
            if (storageSystem[i].contains(fileName)) {
              rvalue = i;
              break;
            }
          }
	  return rvalue;
        }


	/**
	 * Gets the file with the specified name. The time taken (in seconds) for getting the file can
	 * also be found using {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param fileName the name of the needed file
	 * @param i the id of storage system
	 * @return the file with the specified filename
	 */
	public File getFile(String fileName ,int i) {
		return storageSystem[i].getFile(fileName);
	}

	/**
	 * Gets the list of file names located on this storage.
	 * @param i the id of storage system
	 * 
	 * @return a List of file names
	 */
	public List<String> getFileNameList(int i) {
		return storageSystem[i].getFileNameList();
	}

	/**
	 * Adds a file to the storage. First, the method checks if there is enough space on the storage,
	 * then it checks if the file with the same name is already taken to avoid duplicate filenames. <br>
	 * The time taken (in seconds) for adding the file can also be found using
	 * {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file the file to be added
	 * @param i the id of storage system
	 * @return the time taken (in seconds) for adding the specified file
	 */
	public int addFile(File file, int i) {
		int rvalue = -1;
		int id = i;
		if (file == null) {
			Log.printLine("Failed to add file in hybrid storage! FILE_ADD_ERROR_EMPTY");
			return -1;
		}

		if (contains(file.getName())) {
			//for (int j = 0; j < 3; j++)
			//  storageSystem[j].deleteFile(file);
			Log.printLine("Failed to add file in hybrid storage! FILE_ADD_ERROR_EXIST_READ_ONLY");
                        return -1;
		}

		if ( storageSystem == null) {
			Log.printLine("Failed to add file in hybrid storage! FILE_ADD_ERROR_STORAGE_FULL");
                        return -1;
		}

                while (id < 3) {
                  if (storageSystem[id].getAvailableSpace() >= file.getSize()) {
                    storageSystem[id].addFile(file);
                    rvalue = id;
                    break;
                  }
		  id++;
                }
		//Log.printLine("Hybrid storage add file. return id is " + rvalue);
		return rvalue;
	}



	/**
	 * Removes a file from the storage. The time taken (in seconds) for deleting the file can also
	 * be found using {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param fileName the name of the file to be removed
	 * @param i the id of storage system
	 * @return the deleted file
	 */
	public File deleteFile(String fileName, int i) {
		return storageSystem[i].deleteFile(fileName);
	}

	/**
	 * Removes a file from the storage. The time taken (in seconds) for deleting the file can also
	 * be found using {@link gridsim.datagrid.File#getTransactionTime()}.
	 * 
	 * @param file the file which is removed from the storage is returned through this parameter
	 * @param i the id of storage system
	 * @return the time taken (in seconds) for deleting the specified file
	 */
	public double deleteFile(File file, int i) {
		return storageSystem[i].deleteFile(file);
	}

	/**
	 * Checks whether a certain file is on the storage or not.
	 * 
	 * @param fileName the name of the file we are looking for
	 * @return <tt>true</tt> if the file is in the storage, <tt>false</tt> otherwise
	 */
	public boolean contains(String fileName) {
                boolean result = false;
                result = storageSystem[0].contains(fileName) || storageSystem[1].contains(fileName) || storageSystem[2].contains(fileName);
		return result;
	}

	/**
	 * Predict file write time.
	 ** 
	 ** @param fisze the size of output file
	 ** @param i the id of storage system
	 ** @return the double
	 **/
	public double predictFileWriteTime(double fsize, int i) {
            double time = 0.0;
            /*Byte to MB*/
            double writeSize = fsize / (double) Consts.MILLION;
            int id = i;
            double remainSize = storageSystem[id].getAvailableSpace();
            if (remainSize >= writeSize) {
                time += storageSystem[id].getLatency() + writeSize / storageSystem[id].getMaxTransferRate();
            } else {
                time += storageSystem[id].getLatency() + remainSize / storageSystem[id].getMaxTransferRate();
                writeSize -= remainSize;
                /*Performance degradation becaused of interference between write request and backend data movement request*/
                time += storageSystem[id].getLatency() + writeSize  / (storageSystem[id].getMaxTransferRate() / 2);
            }
	    return time;
        }

	/**
	 * @param fisze the size of input file
	 * @param i the id of storage system
	 * @return the double
         */
        public double predictFileReadTime(double fsize, int i) {
          double time = 0.0;
          double maxBwth = storageSystem[i].getMaxTransferRate();
          time = storageSystem[i].getLatency() + fsize / (double) Consts.MILLION / maxBwth;
	  return time;
        }


}
