# DIAMIN
_DIAMIN_ is a high-level software library to facilitate the development of applications for the efficient analysis of large-scale molecular interaction networks. 

## Usage
It runs over Apache Spark (>=2.3,https://spark.apache.org/) and requires a Java compliant virtual machine (>= 1.8). 
The software is released as a single executable jar file, diamin-1.0.0-all.jar, that can be used to process large molecular interaction networks, running in a Local Mode or in a Cluster Mode.

### Local Mode
With Local Mode, _DIAMIN_ employs multiple CPU cores of a single machine. The Local mode requires a proper installation of the Java Development Kit (JDK):
- [JDK 8 Installation for Windows](https://docs.oracle.com/javase/8/docs/technotes/guides/install/windows_jdk_install.html#CHDEBCCJ)
- [JDK 8 Installation for Linux](https://docs.oracle.com/javase/8/docs/technotes/guides/install/linux_jdk.html#BJFGGEFG)
- [JDK 8 Installation for OS X](https://docs.oracle.com/javase/8/docs/technotes/guides/install/mac_jdk.html#CHDBADCG)

The jar file enables the usage of the _DIAMIN_ library from the command line by specifying:
- the **[_input_parameters_]**, that is the list of the parameters required by the _IOmanager_ class to import the interaction network;
- the **_function_name_**, that is a list of functions provided by the _DIAMIN_ library;
- the **[_function_parameters_]**, that is the list of the parameters required by the chosen function.

```
java -jar diamin-1.0.0-all.jar [_input_parameters_] function_name [input_parameters]
```

The [Manual.pdf]() lists and describes in depth all the functions provided by the _DIAMIN_ library. In the following, the command lines to run the examples discussed in the reference paper.

#### Example 1
In a Molecular Interaction Network pivotal interactors are likely to be represented by highly connected nodes (i.e., hubs). 
The _degrees_ function of the _DIAMIN_ library allows the user to extract a subset of interactors, according to the value of their degree. 
This function computes the degrees of each interactor and it returns all those elements satisfying a given condition.
Use the following syntax to compute the interactors of the HomoSapiens_intact_network associated with the 20 largest degrees:
```
java -jar diamin-1.0.0-all.jar LOCAL human_intact_network.txt degree 20
```
Use the following syntax to compute the interactors of the HomoSapiens_string_network associated with the 20 largest degrees:
```
java -jar diamin-1.0.0-all.jar LOCAL human_string_network.txt degree 20
```
#### Example 2
The function with name _xWeightedNeighbors_ returns the x-weighted-Neighborhood of an input node. 
Use the following syntax to compute the x-weighted_neighborhood of the protein TP53 (uniprotkb:P04637) 
w.r.t. the Intact reliability scores for x=0.75:

```
java -jar diamin-1.0.0-all.jar LOCAL human_intact_network.txt xWeightedNeighbors uniprotkb:P04637,0.75
```
Use the following syntax to compute the x-weighted_neighborhood of the protein TP53 (uniprotkb:P04637) 
w.r.t. the Intact reliability scores for x=0.80:

```
java -jar diamin-1.0.0-all.jar LOCAL human_intact_network.txt xWeightedNeighbors uniprotkb:P04637,0.80
```

### Cluster Mode
The Cluster Mode allows to exploit the resources of a computer cluster. Assuming both Apache Spark and Java are properly installed, 
the following syntax allows to perform the degrees computation discussed in the Example 2 on a computer cluster:
```
spark-submit diamin-1.0.0-all.jar CLUSTER human_intact_network.txt degree 20
```
Nowadays, many Cloud Providers enhance a quick and easy creation of a Spark Cluster:
- [Create a cluster with Spark in the Amazon EMR console](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html)

Moreover, we refer the interested reader to the following link for a quick guide about the installation of Apache Spark on a free EC2 AWS instance:
https://dzone.com/articles/apache-spark-setting-up-a-cluster-on-aws.

## Implementation of new functions
_DIAMIN_ also allows user-driven analysis of MIN. In this case, users have to combine the provided function on a Java IDE 
in order to implement new algorithms that solve both standard and more specific problems in network analysis.

### Example: Kleinberg dispersion computation.
IntelliJ IDEA is one of the most famous integrated development environment (IDE) 
for developing computer software written in Java. In the following, we provide  a step-to-step tutorial for the computation of the Kleinberg dispersion 
measure by combining _DIAMIN_ function on IntelliJ IDEA:  
1. Install [IntelliJ IDEA]().
2. Above the list of files of the _DIAMIN_ repository, click on Code button and download the source code.
3. Import the DIAMIN directory in IntelliJ IDEA as a [sbt project]().
4. Combine _DIAMIN_ classes and functions. Write you algorithm in the Main class, starting from line x.
![This is an image](https://myoctocat.com/assets/images/base-octocat.svg)



5. [Build the source code and create an executable jar]().
6. Run your algorithm on local mode.
```
spark-submit diamin-1.0.0-all.jar LOCAL human_intact_network.txt my_algorithm 20
```
7. Run your algorithm on local mode.
```
spark-submit diamin-1.0.0-all.jar CLUSTER human_intact_network.txt my_algorithm 20
```





## Dataset
The _DIAMIN_ library was tested by using the following protein-to-protein network:
- [human_intact_network.txt]()
- [human_string_network.txt]()
