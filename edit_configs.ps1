param (
    [System.Uri]$namenodePath,
    [System.Uri]$datanodePath
 )

$configPath = ".\hadoop\etc\hadoop\"

$xml = [xml](Get-Content ($configPath + "core-site.xml"))
[xml]$newNode = @"
<configuration>
   <property>
       <name>fs.defaultFS</name>
       <value>hdfs://localhost:9000</value>
   </property>
</configuration>
"@
$xml.ReplaceChild($xml.ImportNode($newNode.configuration, $true), $xml.SelectSingleNode("configuration"))
$xml.Save($configPath + "core-site.xml")

$xml = [xml](Get-Content ($configPath + "mapred-site.xml"))
[xml]$newNode = @"
<configuration>
    <property>
       <name>mapreduce.framework.name</name>
       <value>yarn</value>
   </property>
</configuration>
"@
$xml.ReplaceChild($xml.ImportNode($newNode.configuration, $true), $xml.SelectSingleNode("configuration"))
$xml.Save($configPath + "mapred-site.xml")

$xml = [xml](Get-Content ($configPath + "hdfs-site.xml"))
[xml]$newNode = @"
<configuration>
   <property>
       <name>dfs.replication</name>
       <value>1</value>
   </property>
   <property>
       <name>dfs.namenode.name.dir</name>
       <value>FILL_ME_IN</value>
   </property>
   <property>
       <name>dfs.datanode.data.dir</name>
       <value>FILL_ME_IN</value>
   </property>
</configuration>
"@
$newNode.configuration.property[1].value = $namenodePath.AbsoluteUri
$newNode.configuration.property[2].value = $datanodePath.AbsoluteUri
$xml.ReplaceChild($xml.ImportNode($newNode.configuration, $true), $xml.SelectSingleNode("configuration"))
$xml.Save($configPath + "hdfs-site.xml")

$xml = [xml](Get-Content ($configPath + "yarn-site.xml"))
[xml]$newNode = @"
<configuration>
   <property>
    	<name>yarn.nodemanager.aux-services</name>
    	<value>mapreduce_shuffle</value>
   </property>
   <property>
      	<name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>  
	<value>org.apache.hadoop.mapred.ShuffleHandler</value>
   </property>
</configuration>
"@
$xml.ReplaceChild($xml.ImportNode($newNode.configuration, $true), $xml.SelectSingleNode("configuration"))
$xml.Save($configPath + "yarn-site.xml")