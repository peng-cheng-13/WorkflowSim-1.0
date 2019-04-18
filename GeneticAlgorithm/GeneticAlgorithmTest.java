import java.io.File;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Date;
import java.util.Random;
import java.util.HashMap;
import java.util.Iterator;
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


public class GeneticAlgorithmTest extends GeneticAlgorithm {
	public static int jobNum;
 	public static String daxPath;
	public static HashMap<String, Integer> taskType;
	public static HashMap<String, Integer> perTaskFiles;
	public static HashMap<String, String> files2Task;
	

	public GeneticAlgorithmTest(int num) {
		//Two bits represent one storage strategy for one job
		super(num*2);  
	}
	
	@Override
	public int[] changeX(Chromosome chro) {
		return  chro.getNum();
	}

	@Override
	public double caculateY(int[] x) {
		double rvalue =  1000 - Math.log(WorkfowSimulator(x, false));
		return rvalue;
	}

	public static void main(String[] args) {
		daxPath = args[0];
		System.err.println("Processing dax file: " + daxPath);
	        int fileTypeNum = initSimulator();
		System.out.printf("Num of file is %d\n", fileTypeNum);
		GeneticAlgorithmTest test = new GeneticAlgorithmTest(fileTypeNum);
		test.setWorkflowSimInfo(daxPath, taskType, perTaskFiles, files2Task);
		test.caculte();
		int[] storageStrategy = test.bestX();
		WorkfowSimulator(storageStrategy, true);
		System.err.println("Processed dax file: " + daxPath);
	}


  protected static List<CondorVM> createVM(int userId, int vms) {
	LinkedList<CondorVM> list = new LinkedList<>();
	long size = 10000;
	int ram = 512;
        int mips = 1000;
        long bw = 1000;
        int pesNumber = 1;
        String vmm = "Xen";


        CondorVM[] vm = new CondorVM[vms];
        for (int i = 0; i < vms; i++) {
            double ratio = 1.0;
            vm[i] = new CondorVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            list.add(vm[i]);
        }
        return list;
  }

  public static double WorkfowSimulator(int[] storageStrategy, boolean printLog) {
	double totalTime = 0.0;
	try {
            int vmNum = 20;/*number of vms;*/
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                System.exit(0);
            }
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
	    CloudSim mCloudSim = new CloudSim();
            mCloudSim.init(num_user, calendar, trace_flag);
            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");
            datacenter0.setHybridStorage();

            /*Set storageStrategy for jobs. Jobs has the same type shares the same storage strategy*/
            HashMap<String, List<Integer>> perTaskstorage = new HashMap<String, List<Integer>>();
	    int tmpnum = 0;
	    int currentTaskType = 0;
	    int pointer = 0;
	    int tmpfiles = 0;
	    List<Integer> tmpTaskStorage = new ArrayList<>();
	    Iterator<String> iterator = perTaskFiles.keySet().iterator();
	    String tmpTask = null;
	    while (iterator.hasNext()) {
		tmpTaskStorage  = new ArrayList<>();
		tmpTask = iterator.next();
		tmpfiles = perTaskFiles.get(tmpTask);
		for (int j = 0; j < tmpfiles; j++) {
                  tmpTaskStorage.add(storageStrategy[currentTaskType]);
                  currentTaskType++;
                }
		perTaskstorage.put(tmpTask, tmpTaskStorage);
	    }
            datacenter0.setStorageStrategy(perTaskstorage);
	    datacenter0.setFilesToTask(files2Task);

	    /*Ready to simulate*/
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());
            wfEngine.submitVmList(vmlist0, 0);
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
            mCloudSim.startSimulation();
            List<Job> outputList0 = wfEngine.getJobsReceivedList();
            mCloudSim.stopSimulation();
	    for (Job job : outputList0) {
		if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
		  totalTime += job.getActualCPUTime();
		}
	    }
	    /*Print the final storage strategy of each task*/
	    if (printLog) {
		HashMap<String, List<Integer>> finalStorageStrategy = datacenter0.getStorageStrategy();
                printJobList(outputList0, finalStorageStrategy);
	    }

	    /*
            HashMap<String, List<Integer>> finalStorageStrategy = datacenter0.getStorageStrategy();
	    //System.out.printf("Debug!!! jobNum is %d\n", jobNum);
	    iterator = finalStorageStrategy.keySet().iterator();
	    while (iterator.hasNext()) {
		String mytask = iterator.next();
		System.out.printf("perJobStrategy of job %s is %d\n", mytask, finalStorageStrategy.get(mytask).size());
	    }
	    */
        }catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
	return totalTime;
  }

  protected static WorkflowDatacenter createDatacenter(String name) {
        List<Host> hostList = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = 2000;
            peList1.add(new Pe(0, new PeProvisionerSimple(mips)));
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));

            int hostId = 0;
            int ram = 2048;
            long storage = 1000000;
            int bw = 10000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1)));

        }

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";
        double time_zone = 10.0;
        double cost = 3.0;
        double costPerMem = 0.05;
        double costPerStorage = 0.1;
        double costPerBw = 0.1;
        LinkedList<Storage> storageList = new LinkedList<>();
        WorkflowDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        int maxTransferRate = 15;

        try {

            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new WorkflowDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    protected static void printJobList(List<Job> list, HashMap<String, List<Integer>> finalStorageStrategy) {
      try{
        double totalTime = 0.0;
	HashMap<String, Integer> traveredFiles = new HashMap<>();
        String indent = "    ";
        Log.printLine();
	Log.printLine("========== OUTPUT ==========");
	Log.printLine("taskName" + indent + "taskNum" + indent + "inputSize" + indent + "parentNum" + indent
			 + "outputFileNum" + indent + "outputFileID" + indent + "childNum" + indent
			 + "depth" + "fileName" + indent + "fsize" + indent + "storageTier");
	File outfile=new File("testlog.tsv");
	RandomAccessFile raf=new RandomAccessFile(outfile, "rw");
	raf.seek(raf.length());
	for (Job job : list) {
	    if (job.getCloudletStatus() == Cloudlet.FAILED) {
		System.out.println("Job " + job.getTaskList().get(0).getType() + " Failed");
		break;
	    } 
	
	    for (Task task : job.getTaskList()) {
		String taskName =  task.getType();
		if (traveredFiles.containsKey(taskName)) {
		  break;
		} else {
		  traveredFiles.put(taskName, 1);
		}
		int parentNum = task.getParentList().size();
		int childNum = task.getChildList().size();
		int taskNum =  taskType.get(taskName);
		int depth = task.getDepth();
		/*Get input file num*/
		int inputFileNum = 0;
		for (FileItem file : task.getFileList()) {
		    if (file.getType() == FileType.INPUT) {
			inputFileNum++;
		    }
		}
		String tmpNmae = taskName+inputFileNum;
		List<Integer> perFileStorage = finalStorageStrategy.get(tmpNmae);
		if (perFileStorage == null) {
		    break;
		}
		double inputSize = 0;
		int outputFileNum = 0;
		for (FileItem file : task.getFileList()){
		    if (file.getType() == FileType.OUTPUT) {
                        outputFileNum++;
		    } else if (file.getType() == FileType.INPUT) {
			inputSize += file.getSize();
		    }
		}
		int fileid = 0;
		for (FileItem file : task.getFileList()) {
		    if (file.getType() == FileType.OUTPUT) {
			String fileName = file.getName();
			double fsize = file.getSize();
			int storageTier = perFileStorage.get(fileid);
			fileid++;
			//System.out.printf("%s\t%d\t%d\t%d\t%d\t%s\t%d\t%d\n",taskName, taskNum, parentNum, childNum, depth, fileName, fsize, storageTier);
			String sOutput = String.format("%s\t%d\t%.1f\t%d\t%d\t%d\t%d\t%d\t%s\t%.1f\t%d\n",taskName, taskNum, inputSize, parentNum, outputFileNum, fileid, childNum, depth, fileName, fsize, storageTier);
			raf.writeBytes(sOutput);
		    }
		}
	    }
	}
	raf.close();
	/*
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
	*/
	Log.printLine("Append output to file ./testlog.tsv");
      } catch (Exception e) {
	e.printStackTrace();
      }
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
            jobNum = mtask.size();
	    taskType = new HashMap<>();
	    perTaskFiles = new HashMap<>();
	    files2Task = new HashMap<>();
	    
	    String tmpType= null;
	    int typeNum = 0;
	    int inputFileNum = 0;
	    int outputFileNum = 0;
	    /* Each task contains multiple storage strtegies, and each output file of that task maps to one storage strtegy.
	     * Num of total storage strtegies = taskType0*NumofOutputFiles + taskType1*NumofOutputFiles + ... + taskTypeN*NumofOutputFiles
	     * */
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

		/*num of intput and output files of each task*/
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
