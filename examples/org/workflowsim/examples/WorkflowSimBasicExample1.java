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
package org.workflowsim.examples;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Date;
import java.util.Random;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.FileReader;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.HarddriveStorage;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.Job;
import org.workflowsim.FileItem;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.WorkflowParser;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import org.workflowsim.utils.Parameters.ClassType;
import org.workflowsim.utils.Parameters.FileType;

/**
 * This WorkflowSimExample creates a workflow planner, a workflow engine, and
 * one schedulers, one data centers and 20 vms. You should change daxPath at
 * least. You may change other parameters as well.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class WorkflowSimBasicExample1 {

    public static String daxPath;
    public static HashMap<String, Integer> taskType;
    public static HashMap<String, Integer> perTaskFiles;
    public static HashMap<String, String> files2Task;

    protected static List<CondorVM> createVM(int userId, int vms) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<CondorVM> list = new LinkedList<>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 512; //vm memory (MB)
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 4; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        CondorVM[] vm = new CondorVM[vms];
        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        return list;
    }

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {
        try {
            // First step: Initialize the WorkflowSim package. 
            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 20;//number of vms;
            /**
             * Should change this based on real physical path
             */

	    boolean hasInputStorage = false;
            daxPath = args[0];
	    if (args.length == 2) {
		System.out.println("arg1 is " + args[1]);
		String inputStorageFile = args[1];
		File storageFile = new File(inputStorageFile);
		if (storageFile.exists())
		    hasInputStorage = true;
	    }
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using MINMIN scheduling algorithm, the planning
             * algorithm should be INVALID such that the planner would not
             * override the result of the scheduler
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.MINMIN;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            //ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * No overheads
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);

            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            /*Init a tmp parser to get the number of Jobs*/
            WorkflowParser tmpParser = new WorkflowParser(999);
            tmpParser.parse();
	    List<Task> mtask = tmpParser.getTaskList();
            int jobnum = initSimulator();
            Log.printLine("Num of output file type is " + jobnum);
            

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /*Init hybrid storage for datacenter0*/
            datacenter0.setHybridStorage();

            /**
 	     * Init storage strategy.
 	     **/

	    /*Parse storage strategy from prediction file if specified*/
	    int[] storageStrategy = new int[jobnum];
	    if (args.length == 2) {
                System.out.println("arg1 is " + args[1]);
                String inputStorageFile = args[1];
                File storageFile = new File(inputStorageFile);
                if (storageFile.exists()) {
                    hasInputStorage = true;
		    BufferedReader br = new BufferedReader(new FileReader(storageFile));
		    String line = null;
		    /*Leave the first line*/
		    line=br.readLine();
		    int index = 0;
		    while ((line=br.readLine())!=null) {
			String[] segments = line.split("\t");
			/*Parse the last value of each line*/
			int id = segments.length - 1;
			storageStrategy[index] = Integer.parseInt(segments[id]);
			index++;
		    }
		}
            }
	    if (hasInputStorage) {
		System.out.printf("Storage strategy is: ");
		for (int k = 0; k < storageStrategy.length; k++) 
	   	    System.out.printf("%d ", storageStrategy[k]);
	    }

            Date date = new Date();
            long timeMill = date.getTime();
            Random rand = new Random(timeMill);
	    HashMap<String, List<Integer>> perTaskstorage = new HashMap<String, List<Integer>>();
	    int tmpnum = 0;
            int pointer = 0;
            int tmpfiles = 0;
	    int currentTaskType = 0;
	    List<Integer> tmpTaskStorage = new ArrayList<>();
            Iterator<String> iterator = perTaskFiles.keySet().iterator();
            String tmpTask = null;
            while (iterator.hasNext()) {
                tmpTaskStorage  = new ArrayList<>();
                tmpTask = iterator.next();
                tmpfiles = perTaskFiles.get(tmpTask);
                for (int j = 0; j < tmpfiles; j++) {
		  /*Using the predicted storage strategy if exists, else generating storage strategy randomly*/
		  if (hasInputStorage) {
		    tmpTaskStorage.add(storageStrategy[currentTaskType]);
		    currentTaskType++;
		  }
		  else {
                    tmpTaskStorage.add(rand.nextInt(3));
		  }
                }
                perTaskstorage.put(tmpTask, tmpTaskStorage);
            }
	    datacenter0.setStorageStrategy(perTaskstorage);
            datacenter0.setFilesToTask(files2Task);

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());

            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            CloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            CloudSim.stopSimulation();
            printJobList(outputList0);
	    System.out.printf("\n");
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    protected static WorkflowDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= 20; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = 2000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

            int hostId = 0;
            int ram = 2048; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))); // This is our first machine
            //hostId++;
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        WorkflowDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
	double totalTime = 0.0;
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth" + indent + "IputFileSize (MB)" );
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
            Log.print(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == ClassType.STAGE_IN.value) {
                Log.print("Stage-in");
            }
            for (Task task : job.getTaskList()) {
                Log.print(task.getCloudletId() + ",");
            }
            Log.print(indent);

           /*Get input file size for each job*/
           double fileSize = 0;
           List<FileItem> fList = job.getFileList();
           for (FileItem file : fList) {
             fileSize += file.getSize() / 1024 / 1024;
           }

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
		totalTime += job.getActualCPUTime();
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth() + indent + fileSize);
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }
	Log.printLine("Workflow elapsed time is " + totalTime + " s.");
    }

    public static int initSimulator() {
	    File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                System.exit(0);
            }
            int vmNum = 20;
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.MINMIN;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);
            Parameters.init(vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            WorkflowParser tmpParser = new WorkflowParser(999);
            tmpParser.parse();
            List<Task> mtask = tmpParser.getTaskList();
            int jobNum = mtask.size();
            taskType = new HashMap<>();
            perTaskFiles = new HashMap<>();
            files2Task = new HashMap<>();
            String tmpType= null;
            int typeNum = 0;
            int outputFileNum = 0;
	    int inputFileNum = 0;
	    int perFileStrategies = 0;
	    for (int tid = 0; tid < mtask.size(); tid++) {
                tmpType = mtask.get(tid).getType();

                /*num of tasks of each type*/
                if (taskType.containsKey(tmpType)) {
                  typeNum = taskType.get(tmpType) + 1;
                  taskType.put(tmpType, typeNum);
                } else {
                  taskType.put(tmpType, 1);
                }

                /*num of input and output files of each task*/
		inputFileNum = 0;
                outputFileNum = 0;
                if (!perTaskFiles.containsKey(tmpType)) {
                  List<FileItem> fList = mtask.get(tid).getFileList();
                  for (FileItem file : fList) {
                    if (file.getType() == FileType.OUTPUT){
                        outputFileNum++;
                    } else if (file.getType() == FileType.INPUT) {
			inputFileNum++;
		    }
                  }
		  /*System.out.printf("Debug!!! tmpType is %s\n",tmpType+inputFileNum);*/
		  tmpType += inputFileNum;
                  perTaskFiles.put(tmpType, outputFileNum);
                  /*System.out.printf("Debug!!! Task %s has %d output files\n", tmpType, outputFileNum);*/
                  if (outputFileNum != 0) {
                    perFileStrategies += outputFileNum;
                  } else {
                    /*At least one storage strtegy for each task*/
                    perFileStrategies += 1;
                  }
                }

                /*relation between task and files*/
                List<FileItem> fList2 = mtask.get(tid).getFileList();
                String taskFiles = "";
                for (FileItem file : fList2) {
                  taskFiles += file.getName();
                }
                files2Task.put(taskFiles, tmpType);
                //System.out.println("Debug!!! Files of task " + tmpType + " is " + taskFiles);
                }
                return perFileStrategies;
    }
}
