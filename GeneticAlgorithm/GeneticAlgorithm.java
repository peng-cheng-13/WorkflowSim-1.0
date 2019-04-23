import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.*;

public abstract class GeneticAlgorithm {
	private List<Chromosome> population = new ArrayList<Chromosome>();
	private int popSize = 100;
	private int geneSize;
	private int maxIterNum = 100;
	private double mutationRate = 0.1;
	private int maxMutationNum = 20;
	private int generation = 1;
	private double bestScore;
	private double worstScore;
	private double totalScore;
	private double averageScore;
	private int[] x;
	private double y;
	private int geneI;
	
	private int taskSize = 1;
	private String daxPath;
	private HashMap<String, Integer> taskType;
	private HashMap<String, Integer> perTaskFiles;
	private HashMap<String, String> files2Task;

	public GeneticAlgorithm(int geneSize) {
		this.geneSize = geneSize;
	}

	public void caculte() {
		generation = 1;
		init();
		while (generation < maxIterNum) {
			evolve();
			print();
			generation++;
		}
	}

	public int[] bestX() {
	  return x;
	}

	private void print() {
		System.out.println("--------------------------------");
		System.out.println("the generation is:" + generation);
		System.out.println("the best y is:" + bestScore);
		System.out.println("the worst fitness is:" + worstScore);
		System.out.println("the average fitness is:" + averageScore);
		System.out.println("the total fitness is:" + totalScore);
		System.out.println("geneI:" + geneI + "\ty:" + y + "\tx:");
		if (x != null) {
			for (int i = 0; i < x.length; i++) {
				System.out.printf("%d ",x[i]);
			}
			System.out.printf("\n");
		}
	}

	private void init() {
		for (int i = 0; i < popSize; i++) {
			population = new ArrayList<Chromosome>();
			Chromosome chro = new Chromosome(geneSize);
			population.add(chro);
		}
		caculteScore();
	}


	private void evolve() {
		List<Chromosome> childPopulation = new ArrayList<Chromosome>();
		while (childPopulation.size() < popSize) {
			Chromosome p1 = getParentChromosome();
			Chromosome p2 = getParentChromosome();
			List<Chromosome> children = Chromosome.genetic(p1, p2);
			if (children != null) {
				for (Chromosome chro : children) {
					childPopulation.add(chro);
				}
			} 
		}
		
		List<Chromosome> t = population;
		population = childPopulation;
		t.clear();
		t = null;

		mutation();

		caculteScore();
	}

	private Chromosome getParentChromosome (){
		double slice = Math.random() * totalScore;
		double sum = 0;
		for (Chromosome chro : population) {
			sum += chro.getScore();
			if (sum > slice && chro.getScore() >= averageScore) {
				return chro;
			}
		}
		return null;
	}

	private void caculteScore() {
		setChromosomeScore(population.get(0));
		bestScore = population.get(0).getScore();
		worstScore = population.get(0).getScore();
		totalScore = 0;

		// Failed to lauch multiple threads for parallel execution, Class CloudSim contains lots of static methods and variables, static variables are not thread safe.
		/*Init resource pool//
		ExecutorService pool = Executors.newFixedThreadPool(taskSize);
		List<Future> list = new ArrayList<Future>();
		HashMap<Future, Chromosome> tmpRelation = new HashMap<Future, Chromosome>();
		int k = 0;
		for (Chromosome chro : population) {
                       if (chro == null) {
				continue;
			}
			int[] x = changeX(chro);
			Callable c = new CalculateScore(daxPath, taskType, perTaskFiles, files2Task, x, k);
			Future f = pool.submit(c);
			list.add(f);
			tmpRelation.put(f, chro);
			k++;
		}	
		pool.shutdown();
		for (Future f : list) {
			double score = 0;
			try {
			  score = (double) f.get();
			} catch (Exception e) {
			  
			}
			Chromosome chro = tmpRelation.get(f);
			int id = population.indexOf(chro);
			chro.setScore(score);
			population.set(id, chro);
                }

		/*Finish parallel execution*/

		for (Chromosome chro : population) {
			setChromosomeScore(chro);
			if (chro.getScore() > bestScore) {
				bestScore = chro.getScore();
				if (y < bestScore) {
					x = changeX(chro);
					y = bestScore;
					geneI = generation;
				}
			}

			if (chro.getScore() < worstScore) {
				worstScore = chro.getScore();
			}

			totalScore += chro.getScore();
		}

		averageScore = totalScore / popSize;
		averageScore = averageScore > bestScore ? bestScore : averageScore;
	}

	private void mutation()  {
		for (Chromosome chro : population) {
			if (Math.random() < mutationRate) {
				int mutationNum = (int) (Math.random() * maxMutationNum);
				chro.mutation(mutationNum);
			}
		}
	}

	private void setChromosomeScore(Chromosome chro) {
		if (chro == null) {
			return;
		}
		int[] x = changeX(chro);
		double y = caculateY(x);
		chro.setScore(y);
 
	}

       public void setWorkflowSimInfo(String path, HashMap<String, Integer> type, HashMap<String, Integer> files, HashMap<String, String> task) {
	    daxPath = path;
	    taskType = type;
	    perTaskFiles = files;
	    files2Task = task;
       }

	public abstract int[] changeX(Chromosome chro);
	public abstract double caculateY(int[] x);

	public void setPopulation(List<Chromosome> population) {
		this.population = population;
	}
 
	public void setPopSize(int popSize) {
		this.popSize = popSize;
	}
 
	public void setGeneSize(int geneSize) {
		this.geneSize = geneSize;
	}
 
	public void setMaxIterNum(int maxIterNum) {
		this.maxIterNum = maxIterNum;
	}
 
	public void setMutationRate(double mutationRate) {
		this.mutationRate = mutationRate;
	}
 
	public void setMaxMutationNum(int maxMutationNum) {
		this.maxMutationNum = maxMutationNum;
	}
 
	public double getBestScore() {
		return bestScore;
	}
 
	public double getWorstScore() {
		return worstScore;
	}
 
	public double getTotalScore() {
		return totalScore;
	}
 
	public double getAverageScore() {
		return averageScore;
	}
 
	public int[] getX() {
		return x;
	}
 
	public double getY() {
		return y;
	}

}
