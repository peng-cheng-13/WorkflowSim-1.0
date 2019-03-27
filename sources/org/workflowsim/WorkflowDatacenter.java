/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim;

import java.util.Iterator;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.HybridStorage;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.Parameters.FileType;

/**
 * WorkflowDatacenter extends Datacenter so as we can use CondorVM and other
 * components
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowDatacenter extends Datacenter {

    private HybridStorage mHybridStorage = null;
    private int[] mStorageStrategy = null;
    private int currentJobID = 0;

    public WorkflowDatacenter(String name,
            DatacenterCharacteristics characteristics,
            VmAllocationPolicy vmAllocationPolicy,
            List<Storage> storageList,
            double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
    }

    /**
     * Init HybridStorage.
     */
    public void setHybridStorage() {
      try {
        mHybridStorage = new HybridStorage();
      } catch (Exception e) {
         e.printStackTrace();
      }
    }

    /**
     * Init storage strategy.
     */
    public void setStorageStrategy(int[] strategy) {
      mStorageStrategy = strategy;
    }

    public int[] getStorageStrategy() {
      return mStorageStrategy;
    }

    /**
     * Processes a Cloudlet submission. The cloudlet is actually a job which can
     * be cast to org.workflowsim.Job
     *
     * @param ev a SimEvent object
     * @param ack an acknowledgement
     * @pre ev != null
     * @post $none
     */
    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        updateCloudletProcessing();

        try {
            /**
             * cl is actually a job but it is not necessary to cast it to a job
             */
            Job job = (Job) ev.getData();

            if (job.isFinished()) {
                String name = CloudSim.getEntityName(job.getUserId());
                Log.printLine(getName() + ": Warning - Cloudlet #" + job.getCloudletId() + " owned by " + name
                        + " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = job.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(job.getUserId(), tag, data);
                }

                sendNow(job.getUserId(), CloudSimTags.CLOUDLET_RETURN, job);

                return;
            }

            int userId = job.getUserId();
            int vmId = job.getVmId();
            Host host = getVmAllocationPolicy().getHost(vmId, userId);
            CondorVM vm = (CondorVM) host.getVm(vmId, userId);

            switch (Parameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    job.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(),
                            getCharacteristics().getCostPerBw());
                    break;
                case VM:
                    job.setResourceParameter(getId(), vm.getCost(), vm.getCostPerBW());
                    break;
                default:
                    break;
            }
	    currentJobID = job.getCloudletId() - 1;
	    if (currentJobID < 0)
              currentJobID = 0;
            double fileTransferTime = 0.0;

            /**
             * Stage-in file && Shared based on the file.system
             */
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.printLine("Job " + currentJobID + " is STAGE_IN job");
                fileTransferTime += stageInFile2FileSystem(job);
            }

            /**
             * Add data transfer time (communication cost
             */

            if (job.getClassType() == ClassType.COMPUTE.value) {
                fileTransferTime += processDataStageInForComputeJob(job.getFileList(), job);
                Log.printLine("Job ID: " + job.getCloudletId() + " file transferTime is " + fileTransferTime);
            }

            CloudletScheduler scheduler = vm.getCloudletScheduler();
            double estimatedFinishTime = scheduler.cloudletSubmit(job, fileTransferTime);
            updateTaskExecTime(job, vm);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            } else {
                Log.printLine("Warning: You schedule cloudlet to a busy VM");
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = job.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(job.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();
        }
        checkCloudletCompletion();
    }

    /**
     * Update the submission time/exec time of a job
     *
     * @param job
     * @param vm
     */
    private void updateTaskExecTime(Job job, Vm vm) {
        double start_time = job.getExecStartTime();
        for (Task task : job.getTaskList()) {
            task.setExecStartTime(start_time);
            double task_runtime = task.getCloudletLength() / vm.getMips();
            start_time += task_runtime;
            //Because CloudSim would not let us update end time here
            task.setTaskFinishTime(start_time);
        }
    }

    /**
     * Stage in files for a stage-in job. For a local file system (such as
     * condor-io) add files to the local storage; For a shared file system (such
     * as NFS) add files to the shared storage
     *
     * @param cl, the job
     * @pre $none
     * @post $none
     */
    private double stageInFile2FileSystem(Job job) {
        double time = 0.0;
        List<FileItem> fList = job.getFileList();
        try {
          for (FileItem file : fList) {
            ReplicaCatalog.addFileToStorage(file.getName(), this.getName());
            int fsize = (int) file.getSize()/1024/1024;
            int myid = -1;
	    if (fsize == 0)
		fsize = 1;
            if (!mHybridStorage.contains(file.getName())) {
                  myid = mHybridStorage.addFile(new File(file.getName(), fsize), mStorageStrategy[currentJobID]);
		   Log.printLine("Debug!!! Stage in file " + file.getName());
            } else {
                  Log.printLine("Hybrid storage has contained file " + file.getName() + ", do not add again");
            }
            if (myid != -1) {
              //Log.printLine("addFile to storage once");
              mStorageStrategy[currentJobID] = myid;
              time += mHybridStorage.predictFileWriteTime(file.getSize(), myid);
            } else {
               Log.printLine("Failed to add file with return value: " + myid);
            }
          } 
        } catch (Exception e) {
          e.printStackTrace();
        }
	return time;
    }

    /*
     * Stage in for a single job (both stage-in job and compute job)
     * @param requiredFiles, all files to be stage-in
     * @param job, the job to be processed
     * @pre  $none
     * @post $none
     */
    protected double processDataStageInForComputeJob(List<FileItem> requiredFiles, Job job) throws Exception {
        double time = 0.0;
        for (FileItem file : requiredFiles) {
            //The input file is not an output File 
            if (file.isRealInputFile(requiredFiles)) {
                double maxBwth = 0.0;
		
                List siteList = ReplicaCatalog.getStorageList(file.getName());
		if (siteList == null)
		  Log.printLine("siteList is null for file " + file.getName());
                if (siteList.isEmpty()) {
                    throw new Exception(file.getName() + " does not exist");
                }
		
		int storageID = -1;
		if (file.getName() != null) {
         		storageID = mHybridStorage.locateFile(file.getName());
		}

		if (storageID == -1) {
                  throw new Exception(file.getName() + " does not exist in hybrid storage");
                }
		
		if ((storageID == 0) || (storageID == 1)) {
                        int vmId = job.getVmId();
                        int userId = job.getUserId();
                        Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        Vm vm = host.getVm(vmId, userId);

                        boolean requiredFileStagein = true;
                        for (Iterator it = siteList.iterator(); it.hasNext();) {
			    //site is where one replica of this data is located at
                            String site = (String) it.next();
                            if (site.equals(this.getName())) {
                                continue;
                            }
			    //This file is already in the local vm and thus it is no need to transfer
                            if (site.equals(Integer.toString(vmId))) {
                                requiredFileStagein = false;
                                time += mHybridStorage.predictFileReadTime(file.getSize(), storageID);
                                break;
                            }
			    
                            double bwth;

                           if (site.equals(Parameters.SOURCE)) {
			     //transfers from the source to the VM is limited to the VM bw only
                             bwth = vm.getBw();
                           } else {
                             //transfers between two VMs is limited to both VMs
                             bwth = Math.min(vm.getBw(), getVmAllocationPolicy().getHost(Integer.parseInt(site), userId).getVm(Integer.parseInt(site), userId).getBw());
                           }
                           if (bwth > maxBwth) {
                                maxBwth = bwth;
                            }
                        }
			ReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
			
			if (requiredFileStagein) {
				if (mHybridStorage.getMaxTransferRate(storageID) > maxBwth) {
					time += file.getSize() / (double) Consts.MILLION / maxBwth;
				}
				else {
					time += mHybridStorage.predictFileReadTime(file.getSize(), storageID);
				}
			}
		} else if (storageID == 2) {
			ReplicaCatalog.addFileToStorage(file.getName(), this.getName());
			time += mHybridStorage.predictFileReadTime(file.getSize(), 2);
                }
		
            }
        }
        return time;
    }

    @Override
    protected void updateCloudletProcessing() {
        // if some time passed since last processing
        // R: for term is to allow loop at simulation start. Otherwise, one initial
        // simulation step is skipped and schedulers are not properly initialized
        //this is a bug of CloudSim if the runtime is smaller than 0.1 (now is 0.01) it doesn't work at all
        if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + 0.01) {
            List<? extends Host> list = getVmAllocationPolicy().getHostList();
            double smallerTime = Double.MAX_VALUE;
            // for each host...
            for (Host host : list) {
                // inform VMs to update processing
                double time = host.updateVmsProcessing(CloudSim.clock());
                // what time do we expect that the next cloudlet will finish?
                if (time < smallerTime) {
                    smallerTime = time;
                }
            }
            // gurantees a minimal interval before scheduling the event
            if (smallerTime < CloudSim.clock() + 0.11) {
                smallerTime = CloudSim.clock() + 0.11;
            }
            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(CloudSim.clock());
        }
    }

    /**
     * Verifies if some cloudlet inside this PowerDatacenter already finished.
     * If yes, send it to the User/Broker
     *
     * @pre $none
     * @post $none
     */
    @Override
    protected void checkCloudletCompletion() {
        List<? extends Host> list = getVmAllocationPolicy().getHostList();
        for (Host host : list) {
            for (Vm vm : host.getVmList()) {
                while (vm.getCloudletScheduler().isFinishedCloudlets()) {
                    Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
                    if (cl != null) {
                        sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                        //register(cl);
                        mRegister(cl);
                    }
                }
            }
        }
    }

    /**
     * Verifies if some cloudlet inside this PowerDatacenter already finished and return the file write time.
     */
    protected double mCheckCloudletCompletion() {
        double time = 0.0;
        List<? extends Host> list = getVmAllocationPolicy().getHostList();
        for (Host host : list) {
            for (Vm vm : host.getVmList()) {
                while (vm.getCloudletScheduler().isFinishedCloudlets()) {
                    Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
                    if (cl != null) {
                        sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
                        time += mRegister(cl);
                    }
                }
            }
        }
	return time;
    }

    /*
     * Register a file to the storage if it is an output file
     * @param requiredFiles, all files to be stage-in
     * @param job, the job to be processed
     * @pre  $none
     * @post $none
     */

    private void register(Cloudlet cl) {
        Task tl = (Task) cl;
        List<FileItem> fList = tl.getFileList();
        for (FileItem file : fList) {
            if (file.getType() == FileType.OUTPUT)//output file*/
            {
                switch (ReplicaCatalog.getFileSystem()) {
                    case SHARED:
                        ReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                        break;
                    case LOCAL:
                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        /**
                         * Left here for future work
                         */
                        CondorVM vm = (CondorVM) host.getVm(vmId, userId);
                        ReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
                        break;
                }
            }
        }
    }

   /*
    * Register a file to the storage if it is an output file and return the write time.
    */
   private double mRegister(Cloudlet cl) {
        double time = 0.0;
        Task tl = (Task) cl;
        List<FileItem> fList = tl.getFileList();
        for (FileItem file : fList) {
            if (file.getType() == FileType.OUTPUT)//output file*/
            {
		int myid = -1;
                try{
                  int fsize = (int) file.getSize()/1024/1024;
                  if (fsize == 0)
                     fsize = 1;
		  if (!mHybridStorage.contains(file.getName())) {
                    myid = mHybridStorage.addFile(new File(file.getName(), fsize), mStorageStrategy[currentJobID]);
		  }
                  if (myid == 0 || myid == 1 || myid == 2) {
		    //Log.printLine("addFile to storage once with id " + myid);
                    mStorageStrategy[currentJobID] = myid;
                    time = mHybridStorage.predictFileWriteTime(file.getSize(), myid);
                  } 
		  
	  	  switch (myid) {
                    case 0:
                    case 1:
                        int vmId = cl.getVmId();
                        int userId = cl.getUserId();
                        Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        CondorVM vm = (CondorVM) host.getVm(vmId, userId);
                        ReplicaCatalog.addFileToStorage(file.getName(), Integer.toString(vmId));
                        break;
                    case 2:
                        ReplicaCatalog.addFileToStorage(file.getName(), this.getName());
                        break;
                    default:
                        //Log.printLine("Debug!!! Did not add file to ReplicaCatalog: " + file.getName() + "with rvalue " + myid);
                        break;
                   }

                } catch (Exception e) {
                  e.printStackTrace();
                }
            } 
        }
	return time;
    }

}
