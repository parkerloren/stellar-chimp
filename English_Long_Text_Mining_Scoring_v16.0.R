#0202
gc()

#0206-COMMENT ALL SAVE(, WRITE.CSV(, REMOVE ALL LINES CONTAINING READ.XLS

#Set base working directory
baseWD<-"D:\\R_SCRIPTS\\0508"
classPath<-"D:\\Softwares\\RedshiftJDBC41-1.1.10.1010.jar"


#Set up Current time for database entry and log file names
currentTime<-format(Sys.time(), "%Y-%m-%d %H:%M:%S")
currentTime_fName<-gsub("-","_",currentTime)
currentTime_fName<-gsub(" ","_",currentTime_fName)
currentTime_fName<-gsub(":","_",currentTime_fName)

#Enable Logging
LogFile<-paste(baseWD,"\\Log\\",currentTime_fName,"_English_Long_Text_Mining_Scoring_v16.0_Log.txt",sep="")
zz_Log <- file(LogFile, open="wt")
sink(zz_Log, type = "message")

ListFile<-paste(baseWD,"\\Log\\",currentTime_fName,"_English_Long_Text_Mining_Scoring_v16.0_List.txt",sep="")
zz_List <- file(ListFile, open="wt")
sink(zz_List, type = "output")


#Set libraries
library("RSQLite")
library(sqldf)
library(plyr)
library(stringr)
library("RPostgreSQL")
library("RJDBC")
library(tm)   
library(SnowballC)
library(dplyr)
library(stringr)
library(stringdist)
library(zoo)
library(xlsx)
library(qdap)
library(parallel)
library(snow)
#library(Rmpi)
library(stringi)
library(foreach)
library(doSNOW)
library(bigmemory)
library(biganalytics)
library(Matrix)
library(koRpus)
library(hunspell)


#Set Memory Limit
memory.limit()
memory.limit(size=16000000000000)
memory.limit()

#Set up Database Connection
drv_custom1 <- JDBC(driverClass = "com.amazon.redshift.jdbc41.Driver", classPath=classPath)
 
con <- dbConnect(drv_custom1,
                                                url="jdbc:redshift://dapcnvyranalytics.czsqals6k9uo.us-east-1.redshift.amazonaws.com:5439/conveyoranalytics",
                                                dbname = "conveyoranalytics",
                                                schemaname="common_tables",
                                                user = "dapappadmin",
                                                port = 5439,
                                                host = "dapcnvyranalytics.czsqals6k9uo.us-east-1.redshift.amazonaws.com",
                                                password = "Adm1n@aw$")
 
 


#Garbage Collection
gc()

#######################################################################
####0206-Create Error Log Entry and read input data
#######################################################################

#Change the schema
#Change the schema
dbGetQuery(con, "show search_path;")
dbSendUpdate(con, "set search_path to common_tables,public;")
dbGetQuery(con, "show search_path;")

#Create default Error log row
code_name<-paste(baseWD,"\\Code\\English_Long_Text_Mining_Scoring_v16.0.R",sep="")
code_description<-"English Long Text"
run_start_datetime<-currentTime
run_end_datetime<-NA
run_status<-"RUNNING or FAILURE"
run_status_message<-paste("CHECK LOG : ",LogFile, "AND LIST : ",ListFile,SEP="")
errorLogEntry<-data.frame(cbind(code_name,code_description,run_start_datetime,run_status,run_status_message,run_end_datetime))

#Write default Error log row to DB
dbWriteTable(con, "t_error_log", 
             value = errorLogEntry, overwrite=FALSE,append = TRUE, row.names = FALSE)
dbCommit(con)

#Change the schema
dbGetQuery(con, "show search_path;")
dbSendUpdate(con, "set search_path to common_tables,public;")
dbGetQuery(con, "show search_path;")

# Read Target Data
ProductionTable_English <- dbGetQuery(con, "select notification_number,long_text,asset,operation,commodity,commodity_identifier,notification_datetime
						from common_tables.nlp_m_sap_notification_processed
						where  ((notification_datetime >= dateadd(day, -15,current_date) and notification_datetime <= current_date)
						or (notification_changed_date >= dateadd(day, -15,current_date) and notification_changed_date <= current_date))
 						and codingdescr='Event Report'
						and operation is not NULL and asset is not NULL and asset not in ('Pampa Norte','Escondida','')
						and long_text is not NULL")
head(ProductionTable_English)
names(ProductionTable_English)
nrow(ProductionTable_English)

max_notification_datetime <- dbGetQuery(con, "select max(notification_datetime) as max_notification_datetime
						from common_tables.nlp_m_sap_notification_processed
						where  ((notification_datetime >= dateadd(day, -15,current_date) and notification_datetime <= current_date)
						or (notification_changed_date >= dateadd(day, -15,current_date) and notification_changed_date <= current_date))
 						and codingdescr='Event Report'
						and operation is not NULL and asset is not NULL and asset not in ('Pampa Norte','Escondida','')
						and long_text is not NULL")
print(max_notification_datetime)


#######################################################################
####0206-Read Metadata
#######################################################################

#Change the schema
dbGetQuery(con, "show search_path;")
dbSendUpdate(con, "set search_path to naturallanguageprocessing,public;")
dbGetQuery(con, "show search_path;")

Stopwords <- dbGetQuery(con, "select from_text,to_text from naturallanguageprocessing.t_abbreviations where language='English'")
head(Stopwords)
nrow(Stopwords)
colnames(Stopwords)<-c("From","To")
head(Stopwords)

MaterialList <- dbGetQuery(con, "select material,materials_costs,qty,cost_per_qty,equipment from naturallanguageprocessing.t_parts_lookup where language='English'")
head(MaterialList)
nrow(MaterialList)
colnames(MaterialList)<-c("Material","Materials.costs","QTY","Cost.per.qty","Equipment")
head(MaterialList)

Removewords <- dbGetQuery(con, "select remove from naturallanguageprocessing.t_remove_words where language='English'")
head(Removewords)
nrow(Removewords)
colnames(Removewords)<-c("Remove")
head(Removewords)

correctListDB <- dbGetQuery(con, "select * from naturallanguageprocessing.t_correct_words_list where language='English'")
head(correctListDB)
nrow(correctListDB)

n_docs_with_termDB<- dbGetQuery(con, "select * from naturallanguageprocessing.t_n_docs_with_term where language='English' and textlength='Long'")
head(n_docs_with_termDB)
nrow(n_docs_with_termDB)


#######################################################################
####0206-Disconnect from database
#######################################################################

#Disconnect from database
dbDisconnect(con)
 
#Disable DB libraries because these interfere with normal SQLDF calls
detach(package:RPostgreSQL, unload = TRUE)
detach(package:RJDBC, unload = TRUE)

sqldf("select max(notification_datetime) from ProductionTable_English")
sqldf("select min(notification_datetime) from ProductionTable_English")

head(ProductionTable_English$notification_description,100)

#######################################################################
### Initial Cleaning of long text
#######################################################################

#Convert to lower case
ProductionTable_English[,"long_text"]<-tolower(ProductionTable_English[,"long_text"])
head(ProductionTable_English)

#setwd(paste(baseWD,"\\Interim",sep=""))
#write.csv(ProductionTable_English,"ProductionTable_English.csv",row.names=F)

stringCleaned<-character(nrow(ProductionTable_English))

for(i in 1:nrow(ProductionTable_English)){
	string = ProductionTable_English[i,"long_text"]
	strsplit1<-str_split(string,"~0a")[[1]]
	stringCleaned[i]<-paste(strsplit1[-grep("utc",strsplit1)],collapse=" ")
}

head(ProductionTable_English[,"long_text"])
head(stringCleaned)

#Convert to text corpus
long_text1<-Corpus(VectorSource(stringCleaned))
long_text1
as.character(long_text1[[1]])
as.character(long_text1[[2]])
as.character(long_text1[[3]])
as.character(long_text1[[nrow(ProductionTable_English)-2]])
as.character(long_text1[[nrow(ProductionTable_English)-1]])
as.character(long_text1[[nrow(ProductionTable_English)]])

long_text2 <- tm_map(long_text1, function(x) stri_replace_all_fixed(x, "brief description of what occurred", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "basic sequence of events", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "impacts, avoiding the use of personnel names", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "immediate action taken", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "notification raised", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "define work requirements; including known materials, specialist labour", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "special tools that are required", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "define any equipment constraints", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "define any other information to support work requirements", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "scope parts for repair", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "labour requirement: (fitter, electrician, boilermaker etc.)", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "duration", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "special tooling", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "offline/online", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "tasklist", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "scheduled", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "scheduling period", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "materials: (material numbers if possible)", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "completed", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "crew", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "phone", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "support", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "inspection", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "work required", " ", vectorize_all = FALSE))
long_text2 <- tm_map(long_text2, function(x) stri_replace_all_fixed(x, "immediate action taken", " ", vectorize_all = FALSE))


long_text2<-tm_map(long_text2, PlainTextDocument)
long_text2
as.character(long_text2[[1]])
as.character(long_text2[[2]])
as.character(long_text2[[3]])
as.character(long_text2[[nrow(ProductionTable_English)-2]])
as.character(long_text2[[nrow(ProductionTable_English)-1]])
as.character(long_text2[[nrow(ProductionTable_English)]])


#Remove Numbers
long_text3<- tm_map(long_text2, removeNumbers)
long_text3<- tm_map(long_text3, PlainTextDocument)
long_text3
as.character(long_text3[[1]])
as.character(long_text3[[2]])
as.character(long_text3[[3]])
as.character(long_text3[[nrow(ProductionTable_English)-2]])
as.character(long_text3[[nrow(ProductionTable_English)-1]])
as.character(long_text3[[nrow(ProductionTable_English)]])

#Remove Punctuation
replacePunctuation <- content_transformer(function(x) {return (gsub("[[:punct:]]"," ", x))})
long_text4<- tm_map(long_text3, replacePunctuation)
long_text4<- tm_map(long_text4, PlainTextDocument)
long_text4
as.character(long_text4[[1]])
as.character(long_text4[[2]])
as.character(long_text4[[3]])
as.character(long_text4[[nrow(ProductionTable_English)-2]])
as.character(long_text4[[nrow(ProductionTable_English)-1]])
as.character(long_text4[[nrow(ProductionTable_English)]])


#Remove Stopwords
long_text5<- tm_map(long_text4, removeWords, stopwords("english"))
long_text5<- tm_map(long_text5, PlainTextDocument)
long_text5
as.character(long_text5[[1]])
as.character(long_text5[[2]])
as.character(long_text5[[3]])
as.character(long_text5[[nrow(ProductionTable_English)-2]])
as.character(long_text5[[nrow(ProductionTable_English)-1]])
as.character(long_text5[[nrow(ProductionTable_English)]])

#Convert text corpus to dataframe
df_notification_description<-as.data.frame(long_text5)
head(df_notification_description)
names(df_notification_description)
nrow(df_notification_description)

#Join with main data frame
ProductionTable_English$notification_description<-df_notification_description$text
ProductionTable_English<-ProductionTable_English[,!(names(ProductionTable_English) %in% "long_text")]
head(ProductionTable_English)

#Get rid of multiple spaces
ProductionTable_English$notification_description<-gsub('\\s+', ' ',ProductionTable_English$notification_description)
head(ProductionTable_English)
nrow(ProductionTable_English)
names(ProductionTable_English)


#######################################################################
###Short form of words to full form
#######################################################################
#Reading file from Input folder of the directory
getwd()
setwd(paste(baseWD,"\\Input",sep=""))

#Read stop words file
head(Stopwords)
nrow(Stopwords)
tail(Stopwords)

#Remove blank values from Stop words excel
StopwordsV1 <- na.omit(Stopwords)
head(StopwordsV1)
nrow(StopwordsV1)

#Bring to standard format for matching in next steps
StopwordsV1$From<-as.character(str_trim(tolower(StopwordsV1$From)))
StopwordsV1$To<-as.character(str_trim(tolower(StopwordsV1$To)))
head(StopwordsV1)
nrow(StopwordsV1)

#Keep "from" of greater length on top
StopwordsV1<-cbind(StopwordsV1,nchar(StopwordsV1$From))
colnames(StopwordsV1)<-c("From","To","Len_from")
head(StopwordsV1)

StopwordsV1<-sqldf("select * from StopwordsV1 order by Len_from desc")
head(StopwordsV1)
tail(StopwordsV1)

#######################################################################
###Read the material file and get the part details                  ###  
#######################################################################

nrow(MaterialList)


MaterialListV1 <- MaterialList[which(MaterialList$Cost.per.qty > 100),]
head(MaterialListV1)
tail(MaterialListV1)
nrow(MaterialListV1)

#Removing duplicate values from Material List
MaterialListV2 <- MaterialListV1[!duplicated(MaterialListV1[,"Material"]),]
head(MaterialListV2)
nrow(MaterialListV2)


#Bring to standard format for matching in next steps
MaterialListV2$Material<-as.character(str_trim(tolower(MaterialListV2$Material)))
head(MaterialListV2)
nrow(MaterialListV2)

#Remove remove words from material

head(Removewords)
nrow(Removewords)
removewordsList <- as.character(Removewords$Remove)

MaterialListV2<-MaterialListV2[!(MaterialListV2$Material %in% removewordsList),]
head(MaterialListV2)
nrow(MaterialListV2)

#Append leading and trailing space to Material to help with gsub
MaterialListV2$Material_Cleaned<-paste(" ",MaterialListV2$Material," ",sep="")

#Remove all special characters except comma
for(j in 1:nrow(MaterialListV2)){
MaterialListV2[j,"Material_Cleaned"]<-gsub( "[^[:alnum:],]", "",MaterialListV2[j,"Material_Cleaned"])
}
head(MaterialListV2)
tail(MaterialListV2)
nrow(MaterialListV2)

for(j in 1:nrow(MaterialListV2)){
MaterialListV2[j,"Material_Cleaned"]<-paste(" ",gsub(',',' , ',MaterialListV2[j,"Material_Cleaned"])," ",sep="")
}
head(MaterialListV2)
tail(MaterialListV2)
nrow(MaterialListV2)
i=1
#Replace short form in Material List with full forms
for(i in 1:nrow(StopwordsV1))
{
	for(j in 1:nrow(MaterialListV2))
	{
	MaterialListV2$Material_Cleaned[j] <- gsub(paste(' ',str_trim(StopwordsV1[i,"From"]),' ',sep=""),paste(' ',str_trim(StopwordsV1[i,"To"]),' ',sep=""),MaterialListV2[j,"Material_Cleaned"])
	}
}
head(MaterialListV2)
nrow(MaterialListV2)

#Keep "Parts" of greater length on top
MaterialListV3<-cbind(MaterialListV2,nchar(MaterialListV2$Material_Cleaned))
colnames(MaterialListV3)<-c(names(MaterialListV2),"len_mat")
head(MaterialListV3)

MaterialListV3<-sqldf("select * from MaterialListV3 order by len_mat desc")
head(MaterialListV3)

MaterialListV3$Material_Cleaned[1:100]

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(MaterialListV3,file="MaterialListV3.rda")

###LEMMATIZE MATERIAL LIST
head(MaterialListV3)

PartsList<-as.character(MaterialListV3$Material_Cleaned)

PartsListVector<-{}
for(i in 1:length(PartsList)){
PartsListVector<-c(PartsListVector,str_trim(strsplit(PartsList[i],",")[[1]]))
}

PartsListVector<-unique(PartsListVector)
#0202
PartsListVector<-PartsListVector[str_trim(PartsListVector)!=""]

ToLemmaParts<-character(length(PartsListVector))
FromLemmaParts<-character(length(PartsListVector))
for(i in 1:length(PartsListVector)){
	print(PartsListVector[i])
	AEnglish <- treetag(PartsListVector[i], format="obj", treetagger="manual",lang="en", TT.options=list(path="D:/TreeTagger",preset="en"))
	print(taggedText(AEnglish))
	output_current_Vector<-taggedText(AEnglish)$lemma
	output_current_Vector_Check<-output_current_Vector[!(output_current_Vector %in% c(PartsListVector[i],"<unknown>","@card@"))]

	if (length(output_current_Vector_Check)>0){
		ToLemmaParts[i]<-output_current_Vector_Check
		FromLemmaParts[i]<-PartsListVector[i]
	}

}

ToLemmaPartsReduced<-ToLemmaParts[ToLemmaParts!=""]
FromLemmaPartsReduced<-FromLemmaParts[FromLemmaParts!=""]

Material_Cleaned_Lemmatized<-paste(' ',str_trim(as.character(MaterialListV3$Material_Cleaned)),' ',sep="")

for(i in 1:nrow(MaterialListV3))
{
	for(j in 1:length(ToLemmaPartsReduced))
	{
		Material_Cleaned_Lemmatized[i] <- gsub(paste(' ',FromLemmaPartsReduced[j],' ',sep=""),paste(' ',ToLemmaPartsReduced[j],' ',sep=""),Material_Cleaned_Lemmatized[i])
	}
}

MaterialListV4<-cbind(MaterialListV3,Material_Cleaned_Lemmatized)
head(MaterialListV4)
nrow(MaterialListV4)

#Verify
MaterialListV4[str_trim(as.character(MaterialListV4$Material_Cleaned))!=str_trim(as.character(MaterialListV4$Material_Cleaned_Lemmatized)),]
nrow(MaterialListV4[str_trim(as.character(MaterialListV4$Material_Cleaned))!=str_trim(as.character(MaterialListV4$Material_Cleaned_Lemmatized)),])

#0217-START
#Remove "Remove words" after lemmatization
head(MaterialListV4)
head(Removewords)
nrow(Removewords)
removewordsList <- as.character(Removewords$Remove)

nrow(MaterialListV4)
MaterialListV4<-MaterialListV4[!(str_trim(MaterialListV4$Material_Cleaned_Lemmatized) %in% removewordsList),]
nrow(MaterialListV4)
#0217-END

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(MaterialListV4,file="MaterialListV4.rda")


#################################################################
##Data Reduction- Keep only unqiue notifications (events)
#################################################################

names(ProductionTable_English)

#Multiple rows in data correspond to a single event
nrow(ProductionTable_English)
sqldf("select count (distinct notification_number) from ProductionTable_English")


# Collapse the table based on noti number and description
ProductionTable_English$notification_description<-str_trim(as.character(ProductionTable_English$notification_description))
ProductionTable_English_Uniq<-unique(ProductionTable_English[,c("notification_number","notification_description")])
nrow(ProductionTable_English)
nrow(ProductionTable_English_Uniq)

# Keep the description with longer length (in cases where we still have many rows corresponding to an event)
ProductionTable_English_Uniq$Length_desc<-nchar(ProductionTable_English_Uniq$notification_description)
head(ProductionTable_English_Uniq)

ProductionTable_English_Uniq<-sqldf("select * from ProductionTable_English_Uniq order by notification_number,Length_desc desc")
head(ProductionTable_English_Uniq)

ProductionTable_English_Uniq[,"notification_number"]<-str_trim(as.character(ProductionTable_English_Uniq[,"notification_number"]))

Noti_count<-numeric(nrow(ProductionTable_English_Uniq))

Noti_count[1]<-1
for(i in 2:nrow(ProductionTable_English_Uniq)){
	if(ProductionTable_English_Uniq[i,"notification_number"]!=ProductionTable_English_Uniq[i-1,"notification_number"]){
		Noti_count[i]<-1
	} else if(ProductionTable_English_Uniq[i,"notification_number"]==ProductionTable_English_Uniq[i-1,"notification_number"]){
		Noti_count[i]<-Noti_count[i-1]+1
	}
}

#The output below should be 0
sum(Noti_count==0)

#The output below should be = count of unique not
sum(Noti_count==1)
sqldf("select count (distinct notification_number) from ProductionTable_English")

#Join
ProductionTable_English_Uniq1<-cbind(ProductionTable_English_Uniq,Noti_count)
head(ProductionTable_English_Uniq1)

#Just keep count=1 and only 2 columns
ProductionTable_English_Uniq2<-ProductionTable_English_Uniq1[ProductionTable_English_Uniq1$Noti_count==1,c("notification_number","notification_description")]
nrow(ProductionTable_English_Uniq2)
nrow(ProductionTable_English_Uniq2)==sum(Noti_count==1)

head(ProductionTable_English_Uniq2)

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(ProductionTable_English_Uniq2,file="ProductionTable_English_Uniq2.rda")

#################################################################
#################################################################
## TEXT CLEANING
#################################################################
#################################################################

#################################################################
##Replace words in Noti Description
#################################################################

ProductionTable_English_Uniq2[,"notification_description_cleaned"]<-gsub('\\s+', ' ',str_trim(ProductionTable_English_Uniq2[,"notification_description"]))
head(ProductionTable_English_Uniq2[,"notification_description_cleaned"],100)

names(ProductionTable_English_Uniq2)

#Replace short form in Material List with full forms

StopwordsV1[,"From"]<-paste(' ',str_trim(StopwordsV1[,"From"]),' ',sep="")
StopwordsV1[,"To"]<-paste(' ',str_trim(StopwordsV1[,"To"]),' ',sep="")
ProductionTable_English_Uniq2[,"notification_description_cleaned"]<-paste(' ',str_trim(tolower(ProductionTable_English_Uniq2[,"notification_description"])),' ',sep="")

head(ProductionTable_English_Uniq2[,"notification_description_cleaned"],100)

# Just take care of dots for abbrevaition
ProductionTable_English_Uniq2[,"notification_description_cleaned"]<-gsub("\\.", " ", ProductionTable_English_Uniq2[,"notification_description_cleaned"]) 
head(ProductionTable_English_Uniq2[,"notification_description_cleaned"],100)

head(ProductionTable_English_Uniq2[,c("notification_description","notification_description_cleaned")])

head(StopwordsV1)

#Convert to text corpus
notification_description_cleaned2_corpus<-Corpus(VectorSource(ProductionTable_English_Uniq2[,"notification_description_cleaned"]))
notification_description_cleaned2_corpus<- tm_map(notification_description_cleaned2_corpus, PlainTextDocument)
notification_description_cleaned2_corpus
as.character(notification_description_cleaned2_corpus[[1]])
as.character(notification_description_cleaned2_corpus[[2]])
as.character(notification_description_cleaned2_corpus[[nrow(ProductionTable_English_Uniq2)]])

#String replacement
From<-StopwordsV1[,"From"]
To<-StopwordsV1[,"To"]
notification_description_cleaned2_corpus_2 <- tm_map(notification_description_cleaned2_corpus, function(x) stri_replace_all_fixed(x, From, To, vectorize_all = FALSE))
notification_description_cleaned2_corpus_2
as.character(notification_description_cleaned2_corpus_2[[1]])
as.character(notification_description_cleaned2_corpus_2[[2]])
as.character(notification_description_cleaned2_corpus_2[[3]])
as.character(notification_description_cleaned2_corpus_2[[nrow(ProductionTable_English_Uniq2)]])


##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(notification_description_cleaned2_corpus_2,file="notification_description_cleaned2_corpus_2.rda")
#notification_description_cleaned2_corpus_2<-get(load("notification_description_cleaned2_corpus_2.rda"))

#################################################################
##Remove Numbers
#################################################################
LogData1<- tm_map(notification_description_cleaned2_corpus_2, removeNumbers)
LogData1<- tm_map(LogData1, PlainTextDocument)
LogData1
as.character(LogData1[[1]])
as.character(LogData1[[2]])
as.character(LogData1[[3]])
as.character(LogData1[[nrow(ProductionTable_English_Uniq2)-2]])
as.character(LogData1[[nrow(ProductionTable_English_Uniq2)-1]])
as.character(LogData1[[nrow(ProductionTable_English_Uniq2)]])

#################################################################
##Remove Punctuation
#################################################################

replacePunctuation <- content_transformer(function(x) {return (gsub("[[:punct:]]"," ", x))})
LogData2<- tm_map(LogData1, replacePunctuation)
LogData2<- tm_map(LogData2, PlainTextDocument)
LogData2
as.character(LogData2[[1]])
as.character(LogData2[[2]])
as.character(LogData2[[3]])
as.character(LogData2[[nrow(ProductionTable_English_Uniq2)-2]])
as.character(LogData2[[nrow(ProductionTable_English_Uniq2)-1]])
as.character(LogData2[[nrow(ProductionTable_English_Uniq2)]])

#################################################################
##Remove Stopwords
#################################################################
LogData3<- tm_map(LogData2, removeWords, stopwords("english"))
LogData3<- tm_map(LogData3, PlainTextDocument)
LogData3
as.character(LogData3[[1]])
as.character(LogData3[[2]])
as.character(LogData3[[3]])
as.character(LogData3[[nrow(ProductionTable_English_Uniq2)-2]])
as.character(LogData3[[nrow(ProductionTable_English_Uniq2)-1]])
as.character(LogData3[[nrow(ProductionTable_English_Uniq2)]])

#################################################################
##String Replacement Again
#################################################################
From<-StopwordsV1[,"From"]
To<-StopwordsV1[,"To"]
LogData3 <- tm_map(LogData3, function(x) stri_replace_all_fixed(x, From, To, vectorize_all = FALSE))
LogData3<- tm_map(LogData3, PlainTextDocument)
as.character(LogData3[[1]])
as.character(LogData3[[2]])
as.character(LogData3[[3]])
as.character(LogData3[[nrow(ProductionTable_English_Uniq2)-2]])
as.character(LogData3[[nrow(ProductionTable_English_Uniq2)-1]])
as.character(LogData3[[nrow(ProductionTable_English_Uniq2)]])


##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(LogData3,file="LogData3.rda")

#LogData3<-get(load("LogData3.rda"))

#################################################################
##get remove words- to omit them from spell check and lammetization
#################################################################

#Reading remove words file from Input folder of the directory
getwd()
setwd(paste(baseWD,"\\Input",sep=""))

#Read stop words file

head(Removewords)
nrow(Removewords)
removewordsList <- as.character(Removewords$Remove)
#removewordsList <- paste(" ",as.character(Removewords$Remove)," ",sep="")

#################################################################
## Spell Check
#################################################################

## create a term document matrix for spell check
dtm_spell <- DocumentTermMatrix(LogData3)
ncol(dtm_spell)
nrow(dtm_spell)

##Get list of words in the DTM
list_for_spell_check<-names(data.frame(inspect(dtm_spell[1,])))
length(list_for_spell_check)

##Remove removewordsList
list_for_spell_check<-list_for_spell_check[!(list_for_spell_check %in% removewordsList)]
length(list_for_spell_check)

#Keep only words greater than 4
list_for_spell_check<-list_for_spell_check[nchar(str_trim(list_for_spell_check))>4]
length(list_for_spell_check)

## Find Mis-spelled words
incorrectList<-list_for_spell_check[!hunspell_check(list_for_spell_check,dict="en_US")]

#Modify incorrect list
incorrectListReduced<-incorrectList[incorrectList!=""]

#0217
print("###SPELL CHECK###")
print("###INCORRECT WORDS###")
print(incorrectListReduced)


head(MaterialListV4)

List_Material<-c(as.character(MaterialListV4$Material_Cleaned),as.character(MaterialListV4$Material_Cleaned_Lemmatized))
List_Material<-unique(str_trim(List_Material))

#Create Correct List
List_Material_spell<-{}
for(l in 1:length(List_Material)){
List_Material_spell<-c(List_Material_spell,str_trim(str_split(str_trim(List_Material),"\\,")[[l]]))
}
List_Material_spell<-List_Material_spell[order(List_Material_spell)]

correctList<-list_for_spell_check[!(list_for_spell_check %in% incorrectListReduced)]

#0206 START- Add correctListDB
head(correctListDB)
nrow(correctListDB)
correctListModified<-c(correctListDB$correctlistmodified,correctList,List_Material_spell)
correctListModified<-unique(tolower(correctListModified))

new_correct_words<-correctListModified[!(correctListModified %in% correctListDB$correctlistmodified)]

new_correct_words<-unique(tolower(new_correct_words))
new_correct_words<-new_correct_words[!(str_trim(new_correct_words) %in% "")]

correctListTab<-data.frame(new_correct_words)
head(correctListTab)

if (nrow(correctListTab)>0){
correctListTab$Language<-"English"
correctListTab$textLength<-"Long"
head(correctListTab)

#Save it for inserting later in database
setwd(paste(baseWD,"\\Interim",sep=""))
save(correctListTab,file="correctListTab.rda")

setwd(paste(baseWD,"\\Backup",sep=""))
save(correctListTab,file=paste(currentTime_fName,"_EN_LONG_correctListTab.rda",sep=""))
}


#0206 END- Add correctListDB



#get Spell Check suggestion
incorrect_and_suggestions_list<-character(2*length(incorrectListReduced))
for(k in 1:length(incorrectListReduced)){
	suggestions<-hunspell_suggest(incorrectListReduced[k],dict="en_US")[[1]]
	incorrect_and_suggestions_list[2*k-1]<-incorrectListReduced[k]
	incorrect_and_suggestions_list[2*k]<-paste(suggestions[(suggestions %in% correctListModified)],collapse=" ")
}

From<-character(length(incorrectListReduced))
To<-character(length(incorrectListReduced))
for(f in 1:length(incorrectListReduced)){
	From[f]<-paste(" ",incorrect_and_suggestions_list[2*f-1]," ",sep="")
	To[f]<-paste(" ",incorrect_and_suggestions_list[2*f]," ",sep="")
}


length(From)
length(To)
length(From)==length(To)

To_Reduced_Spell<-To[!(str_trim(To) %in% '')]
From_Reduced_Spell<-From[!(str_trim(To) %in% '')]

length(From_Reduced_Spell)
length(To_Reduced_Spell)
length(From_Reduced_Spell)==length(To_Reduced_Spell)

#Check if To is missing
To_Reduced_Spell<-To_Reduced_Spell[!(str_trim(To_Reduced_Spell)=="")]
From_Reduced_Spell<-From_Reduced_Spell[!(str_trim(To_Reduced_Spell)=="")]

length(From_Reduced_Spell)
length(To_Reduced_Spell)

#0217
print("###SPELL CHECK###")
print("###SUGGESTIONS###")
print("### FROM ###")
print(From_Reduced_Spell)
print("### TO ###")
print(To_Reduced_Spell)
from_to1<-cbind(From_Reduced_Spell,To_Reduced_Spell)
print(from_to1)
head(from_to1,50)

To_Reduced_Spell<-To_Reduced_Spell[nchar(str_trim(From_Reduced_Spell))>4]
From_Reduced_Spell<-From_Reduced_Spell[nchar(str_trim(From_Reduced_Spell))>4]

length(From_Reduced_Spell)
length(To_Reduced_Spell)

#0217
print("###SPELL CHECK###")
print("###SUGGESTIONS WITH MORE THAN 4 CHARS###")
print("### FROM ###")
print(From_Reduced_Spell)
print("### TO ###")
print(To_Reduced_Spell)
from_to2<-cbind(From_Reduced_Spell,To_Reduced_Spell)
print(from_to2)
head(from_to2,50)

# Suggested words should be close to word being replaced
To_Reduced_Spell_New<-character(length(From_Reduced_Spell))
From_Reduced_Spell_New<-character(length(From_Reduced_Spell))
for(f in 1:length(From_Reduced_Spell)){
	fromLen<-nchar(str_trim(From_Reduced_Spell[f]))
	toCurr<-str_split(str_trim(To_Reduced_Spell[f])," ")[[1]]

	toString<-{}
	for(s in 1:length(toCurr)){
		#0217
		if(adist(toCurr[s],str_trim(From_Reduced_Spell[f]))<=ceiling(fromLen*.20)
			&&
			(substr(toCurr[s],1,1)==substr(str_trim(From_Reduced_Spell[f]),1,1))
		){
			#0217
			toString<-str_trim(paste(toString,toCurr[s],collapse=" "))
		}
	}
	
	if(length(toString)>0){
		To_Reduced_Spell_New[f]<-toString
		From_Reduced_Spell_New[f]<-str_trim(From_Reduced_Spell[f])
	}

}

#Check if To is missing
To_Reduced_Spell_New2<-To_Reduced_Spell_New[!(str_trim(To_Reduced_Spell_New)=="")]
From_Reduced_Spell_New2<-From_Reduced_Spell_New[!(str_trim(To_Reduced_Spell_New)=="")]

length(To_Reduced_Spell_New2)
length(From_Reduced_Spell_New2)
length(From_Reduced_Spell_New2)==length(To_Reduced_Spell_New2)

#0217
print("###SPELL CHECK###")
print("###CLOSE SUGGESTIONS###")
print("### FROM ###")
print(From_Reduced_Spell_New2)
print("### TO ###")
print(To_Reduced_Spell_New2)
from_to3<-data.frame(cbind(From_Reduced_Spell_New2,To_Reduced_Spell_New2))
print(from_to3)
print(from_to3[as.character(from_to3$From_Reduced_Spell_New2) %in% str_trim(From_Reduced_Spell[1:50]),])
head(from_to3,50)


From_Reduced_Spell_New2<-paste(' ',str_trim(From_Reduced_Spell_New2),' ',sep="")
To_Reduced_Spell_New2<-paste(' ',str_trim(To_Reduced_Spell_New2),' ',sep="")

head(From_Reduced_Spell_New2)
head(To_Reduced_Spell_New2)
tail(From_Reduced_Spell_New2)
tail(To_Reduced_Spell_New2)

setwd(paste(baseWD,"\\Interim",sep=""))
#save(From_Reduced_Spell_New2,file="From_Reduced_Spell_New2.rda")
#save(To_Reduced_Spell_New2,file="To_Reduced_Spell_New2.rda")

LogData4 <- tm_map(LogData3, function(x) stri_replace_all_fixed(x, From_Reduced_Spell_New2, To_Reduced_Spell_New2, vectorize_all = FALSE))
LogData4<-tm_map(LogData4, PlainTextDocument)
LogData4
as.character(LogData4[[1]])
as.character(LogData4[[2]])
as.character(LogData4[[3]])
as.character(LogData4[[4]])
as.character(LogData4[[5]])
as.character(LogData4[[6]])
as.character(LogData4[[16]])
as.character(LogData4[[nrow(ProductionTable_English_Uniq2)-2]])
as.character(LogData4[[nrow(ProductionTable_English_Uniq2)-1]])
as.character(LogData4[[nrow(ProductionTable_English_Uniq2)]])

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(LogData4,file="LogData4.rda")

#################################################################
## Lemmatization
#################################################################

## create a term document matrix for Lemmatization
dtm_lemma_text <- DocumentTermMatrix(LogData4)
ncol(dtm_lemma_text)
nrow(dtm_lemma_text)

##Get list of words in the DTM
list_for_lemma_text<-names(data.frame(inspect(dtm_lemma_text[1,])))

length(list_for_lemma_text)
##Remove removewordsList
list_for_lemma_text<-list_for_lemma_text[!(list_for_lemma_text %in% removewordsList)]
length(list_for_lemma_text)
#0202
list_for_lemma_text<-list_for_lemma_text[str_trim(list_for_lemma_text)!=""]

ToLemma<-character(length(list_for_lemma_text))
FromLemma<-character(length(list_for_lemma_text))

for(i in 1:length(list_for_lemma_text)){
	AEnglish <- treetag(list_for_lemma_text[i], format="obj", treetagger="manual",lang="en", TT.options=list(path="D:/TreeTagger",preset="en"))
	#print(taggedText(AEnglish))
	output_current_Vector<-taggedText(AEnglish)$lemma
	output_current_Vector_Check<-output_current_Vector[!(output_current_Vector %in% c(list_for_lemma_text[i],"<unknown>","@card@"))]

	if (length(output_current_Vector_Check)>0){
		ToLemma[i]<-paste(output_current_Vector_Check,sep=" ")
		FromLemma[i]<-list_for_lemma_text[i]
	}

}


ToLemma_Reduced<-ToLemma[!(ToLemma%in% c("","."))]
FromLemma_Reduced<-FromLemma[!(ToLemma%in% c("","."))]

length(ToLemma_Reduced)
length(FromLemma_Reduced)

ToLemma_Reduced<-paste(" ",ToLemma_Reduced," ",sep="")
FromLemma_Reduced<-paste(" ",FromLemma_Reduced," ",sep="")

head(FromLemma_Reduced)
head(ToLemma_Reduced)
tail(FromLemma_Reduced)
tail(ToLemma_Reduced)

length(ToLemma_Reduced)
length(FromLemma_Reduced)

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(FromLemma_Reduced,file="FromLemma_Reduced.rda")
#save(ToLemma_Reduced,file="ToLemma_Reduced.rda")

LogData5 <- tm_map(LogData4, function(x) stri_replace_all_fixed(x, FromLemma_Reduced, ToLemma_Reduced, vectorize_all = FALSE))
LogData5<-tm_map(LogData5, PlainTextDocument)
LogData5
as.character(LogData5[[1]])
as.character(LogData5[[2]])
as.character(LogData5[[3]])
as.character(LogData5[[4]])
as.character(LogData5[[5]])
as.character(LogData5[[6]])
as.character(LogData5[[16]])
as.character(LogData5[[nrow(ProductionTable_English_Uniq2)-2]])
as.character(LogData5[[nrow(ProductionTable_English_Uniq2)-1]])
as.character(LogData5[[nrow(ProductionTable_English_Uniq2)]])

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(LogData5,file="LogData5.rda")

#################################################################
##Find Parts in Noti Description
#################################################################

#Convert text corpus to dataframe
df_corpus<-as.data.frame(LogData5)
head(df_corpus)
tail(df_corpus)

#Verify
nrow(df_corpus)
LogData5

head(ProductionTable_English_Uniq2)
ProductionTable_English_Uniq3<-cbind(ProductionTable_English_Uniq2,df_corpus$text)
colnames(ProductionTable_English_Uniq3)<-c(names(ProductionTable_English_Uniq2),"notification_description_cleaned2")
head(ProductionTable_English_Uniq3)

#Sort parts of data in descending order of cost so as to have only costliest part in cluster name (in case of multi part cluster)
head(MaterialListV4)
MaterialListV4<-MaterialListV4[rev(order(MaterialListV4$Cost.per.qty)),]
head(MaterialListV4)
tail(MaterialListV4)

#Sort parts in alphabetical order
#head(MaterialListV3)
#MaterialListV3<-MaterialListV3[order(MaterialListV3$Material_Cleaned),]
#head(MaterialListV3)
#tail(MaterialListV3)

List_Material<-unique(as.character(MaterialListV4$Material_Cleaned_Lemmatized))
clusterNameParts<-character(nrow(ProductionTable_English_Uniq3))
notification_description_cleaned3<-as.character(ProductionTable_English_Uniq3$notification_description_cleaned2)

for(i in 1:nrow(ProductionTable_English_Uniq3))
{
countSearchMultiple<-0
lenclusterNameParts<-0
clusterNameParts[i]<-""
	for(j in 1:length(List_Material))
	{
		
		countSearch<-0
		searchPart<-strsplit(List_Material[j],",",fixed=T)[[1]]
		length(searchPart)

		for (s in 1:length(searchPart)){
			if (length(grep(searchPart[s],notification_description_cleaned3[i]))>0){
				countSearch=countSearch+1
			}
		}
		
		if (countSearch>1){
			countSearchMultiple<-1
			clusterNameParts[i]<-List_Material[j]
		}

		else if (countSearchMultiple==0 && countSearch==length(searchPart) && lenclusterNameParts<=19){
			clusterNameParts[i]<-paste(clusterNameParts[i],List_Material[j],sep="+")
			lenclusterNameParts<-length(str_split(clusterNameParts[i],"\\+")[[1]])-1
			
		}
	}
	if(countSearchMultiple==0){
	current_parts<-str_split(clusterNameParts[i],"\\+")[[1]][-1]
	clusterNameParts[i]<-paste(current_parts[order(current_parts)],collapse="+")
	}
}

ProductionTable_English_Uniq4<-cbind(ProductionTable_English_Uniq3,notification_description_cleaned3,clusterNameParts)
#setwd(paste(baseWD,"\\Interim",sep=""))
#write.csv(ProductionTable_English_Uniq4,"ProductionTable_English_Uniq4.csv")
##save(ProductionTable_English_Uniq4,file="ProductionTable_English_Uniq4.rda")

#setwd(paste(baseWD,"\\Interim",sep=""))
#ProductionTable_English_Uniq4<-get(load("ProductionTable_English_Uniq4.rda"))
head(ProductionTable_English_Uniq4)
names(ProductionTable_English_Uniq4)
nrow(ProductionTable_English_Uniq4)



#clusterNameParts[1:39194]
#notification_description_cleaned2[39062]
#clusterNameParts[39062]

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(ProductionTable_English_Uniq4,file="ProductionTable_English_Uniq4.rda")


#################################################################
## Separate the events that were named using parts
#################################################################
#setwd(paste(baseWD,"\\Interim",sep=""))
#getwd()
#ProductionTable_English_Uniq4<-get(load("ProductionTable_English_Uniq4.rda"))
#head(ProductionTable_English_Uniq4)
#nrow(ProductionTable_English_Uniq4)

#Separate the events that were named using parts
head(ProductionTable_English_Uniq4)
nrow(ProductionTable_English_Uniq4)
eventsNamedPartsFinal<-ProductionTable_English_Uniq4[str_trim(ProductionTable_English_Uniq4$clusterNameParts)!="",]
nrow(eventsNamedPartsFinal)
head(eventsNamedPartsFinal,100)

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(eventsNamedPartsFinal,file="eventsNamedPartsFinal.rda")

#################################################################
## Separate the events to be named using TF-IDF
#################################################################

#Identify events that have not been named using parts
names(ProductionTable_English_Uniq4)
head(ProductionTable_English_Uniq4)
#0217
clustData1<-ProductionTable_English_Uniq4[str_trim(ProductionTable_English_Uniq4$clusterNameParts)=="",]
#clustData1<-ProductionTable_English_Uniq4
nrow(clustData1)

#Verify
nrow(ProductionTable_English_Uniq4)==nrow(eventsNamedPartsFinal)+nrow(clustData1)

#Check if We still have multiple rows for a Noti- SHOULDN'T BE
sqldf("select count(distinct notification_number) from clustData1")==nrow(clustData1)

head(clustData1)
##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(clustData1,file="clustData1.rda")

#clustData1<-get(load("clustData1.rda"))
#################################################################
## #0206- START ENTIRE SECTION CHANGED Text cleaning for naming using TF-IDF
#################################################################
head(clustData1)
nrow(clustData1)

###########FIND NEW WORDS IN EXECUTION WINDOW################ 

#Split row identifier and text
RowDF<-data.frame(clustData1[,"notification_number"])
colnames(RowDF)<-c("notification_number")
head(RowDF)

LogDF<-data.frame(clustData1[,"notification_description_cleaned3"])
colnames(LogDF)<-c("Summary")
head(LogDF)


##Convert text dataframe into text corpus
LogData6_TFIDF<-Corpus(VectorSource(LogDF$Summary))
LogData6_TFIDF<- tm_map(LogData6_TFIDF, PlainTextDocument)
LogData6_TFIDF
as.character(LogData6_TFIDF[[1]])
as.character(LogData6_TFIDF[[2]])
as.character(LogData6_TFIDF[[3]])
as.character(LogData6_TFIDF[[4]])
as.character(LogData6_TFIDF[[5]])
as.character(LogData6_TFIDF[[6]])
as.character(LogData6_TFIDF[[nrow(LogDF)-2]])
as.character(LogData6_TFIDF[[nrow(LogDF)-1]])
as.character(LogData6_TFIDF[[nrow(LogDF)]])

## create a term document matrix
dtm_execution <- DocumentTermMatrix(LogData6_TFIDF)
ncol(dtm_execution)
nrow(dtm_execution)

dtm_matrix_execution <-as.matrix(dtm_execution)
ncol(dtm_matrix_execution)
nrow(dtm_matrix_execution)

wordList_execution<-colnames(dtm_matrix_execution)

#New words that DO NOT exist in metadata table
new_words_execution<-wordList_execution[!(str_trim(wordList_execution) %in% str_trim(n_docs_with_termDB$wordlist))]
head(new_words_execution)
length(new_words_execution)

if(length(new_words_execution)==0){
new_words_execution<-""
}

#Find n docs with new terms
n_docs_with_term_new_execution<-numeric(length(new_words_execution))

for(j in 1:length(new_words_execution)){
	n_docs_with_term_new_execution[j]<-sum(dtm_matrix_execution[,j]>0)
}

Tab_n_docs_with_term_new_execution<-data.frame(cbind(new_words_execution,n_docs_with_term_new_execution))


###########FIND EXISTING WORDS IN NEW WINDOW################ 

## Find new window ##
head(clustData1)
nrow(clustData1)

#bring noti date
clustData1_noti_dt<-merge(x=clustData1,y=ProductionTable_English[,c("notification_number","notification_datetime")],by="notification_number",all.x=TRUE)
head(clustData1_noti_dt)
nrow(clustData1_noti_dt)
nrow(clustData1)==nrow(clustData1_noti_dt)

#Convert Noti date to numeric
clustData1_noti_dt$notification_datetime_num<-as.numeric(as.POSIXct(clustData1_noti_dt$notification_datetime, format="%Y-%m-%d  %H:%M:%S"))
head(clustData1_noti_dt)


if(nrow(n_docs_with_termDB)>0){
	#Find cut_off date
	head(n_docs_with_termDB)
	cut_off_date<-max(n_docs_with_termDB$cutoff_notification_datetime)
	cut_off_date_num<-as.numeric(as.POSIXct(cut_off_date, format="%Y-%m-%d  %H:%M:%S"))
} else {
	cut_off_date_num<-0
}

#New Window
clustData2<-clustData1_noti_dt[clustData1_noti_dt$notification_datetime_num>cut_off_date_num,]
head(clustData2)
nrow(clustData2)
nrow(clustData1)


if(nrow(clustData2)==0){

	for(n in 1:length(names(clustData2))){
		clustData2[1,names(clustData2)[n]]<-""
	}
	clustData2$notification_description_cleaned3<-as.character(clustData2$notification_description_cleaned3)
	clustData2$notification_description_cleaned2<-as.character(clustData2$notification_description_cleaned2)
	clustData2$notification_description_cleaned3<-"x"
}
nrow(clustData2)
head(clustData2)

##FIND EXISTING WORDS IN NEW WINDOW

#Split row identifier and text
RowDFn<-data.frame(clustData2[,"notification_number"])
colnames(RowDFn)<-c("notification_number")
head(RowDFn)

LogDFn<-data.frame(clustData2[,"notification_description_cleaned3"])
colnames(LogDFn)<-c("Summary")
head(LogDFn)


##Convert text dataframe into text corpus
LogData7_TFIDF<-Corpus(VectorSource(LogDFn$Summary))
LogData7_TFIDF<- tm_map(LogData7_TFIDF, PlainTextDocument)
LogData7_TFIDF
as.character(LogData7_TFIDF[[1]])
as.character(LogData7_TFIDF[[nrow(LogDFn)]])


## create a term document matrix
dtm_new <- DocumentTermMatrix(LogData7_TFIDF)
ncol(dtm_new)
nrow(dtm_new)

dtm_matrix_new <-as.matrix(dtm_new)
ncol(dtm_matrix_new)
nrow(dtm_matrix_new)

wordList_new<-colnames(dtm_matrix_new)

#existing words that exist in metadata table
existing_words_new<-wordList_new[(str_trim(wordList_new) %in% str_trim(n_docs_with_termDB$wordlist))]
head(existing_words_new)
length(existing_words_new)

if(length(existing_words_new)==0){
existing_words_new<-""
}

#Find n docs with existing terms
n_docs_with_term_existing_new<-numeric(length(existing_words_new))

if(ncol(dtm_matrix_new)!=0){
for(j in 1:length(existing_words_new)){
	n_docs_with_term_existing_new[j]<-sum(dtm_matrix_new[,j]>0)
}
}

Tab_n_docs_with_term_existing_new<-data.frame(cbind(existing_words_new,n_docs_with_term_existing_new))
head(Tab_n_docs_with_term_existing_new)
nrow(Tab_n_docs_with_term_existing_new)

###########UPDATE n_docs_with_termDB with existing words in new window ################
head(n_docs_with_termDB)
nrow(n_docs_with_termDB)

head(Tab_n_docs_with_term_existing_new)
names(Tab_n_docs_with_term_existing_new)
colnames(Tab_n_docs_with_term_existing_new)<-c("wordlist","n_docs_with_term_existing_new")
names(Tab_n_docs_with_term_existing_new)
nrow(Tab_n_docs_with_term_existing_new)

n_docs_with_termDB1<-merge(x=n_docs_with_termDB,y=Tab_n_docs_with_term_existing_new,by="wordlist",all.x=TRUE)
nrow(n_docs_with_termDB1)
nrow(n_docs_with_termDB)==nrow(n_docs_with_termDB1)
head(n_docs_with_termDB1)

#Verify
head(n_docs_with_termDB1[!is.na(n_docs_with_termDB1$n_docs_with_term_existing_new),])
nrow(n_docs_with_termDB1[!is.na(n_docs_with_termDB1$n_docs_with_term_existing_new),])
nrow(n_docs_with_termDB1[!is.na(n_docs_with_termDB1$n_docs_with_term_existing_new),])==nrow(Tab_n_docs_with_term_existing_new)
table(n_docs_with_termDB1$n_docs_with_term_existing_new)

n_docs_with_termDB1$n_docs_with_term_existing_new<-as.character(n_docs_with_termDB1$n_docs_with_term_existing_new)
n_docs_with_termDB1$n_docs_with_term_existing_new[is.na(n_docs_with_termDB1$n_docs_with_term_existing_new)]<-0

#Verify
head(n_docs_with_termDB1)
head(n_docs_with_termDB1[(n_docs_with_termDB1$n_docs_with_term_existing_new!=0),])
nrow(n_docs_with_termDB1[(n_docs_with_termDB1$n_docs_with_term_existing_new!=0),])
table(n_docs_with_termDB1$n_docs_with_term_existing_new)

#++ n_docs_with_term for existing docs
head(n_docs_with_termDB1)
n_docs_with_termDB1$n_docs_with_term<-as.numeric(as.character(n_docs_with_termDB1$n_docs_with_term))
n_docs_with_termDB1$n_docs_with_term_existing_new<-as.numeric(as.character(n_docs_with_termDB1$n_docs_with_term_existing_new))
n_docs_with_termDB1$n_docs_with_term_new<-n_docs_with_termDB1$n_docs_with_term+n_docs_with_termDB1$n_docs_with_term_existing_new


#Verify
head(n_docs_with_termDB1[n_docs_with_termDB1$n_docs_with_term_new!=n_docs_with_termDB1$n_docs_with_term,],20)
nrow(n_docs_with_termDB1[n_docs_with_termDB1$n_docs_with_term_new!=n_docs_with_termDB1$n_docs_with_term,])
nrow(n_docs_with_termDB1[n_docs_with_termDB1$n_docs_with_term_new!=n_docs_with_termDB1$n_docs_with_term,])==nrow(Tab_n_docs_with_term_existing_new)


#Bring table to standard format

n_docs_with_termDB1$n_docs_with_term<-n_docs_with_termDB1$n_docs_with_term_new
n_docs_with_termDB1<-n_docs_with_termDB1[,!(names(n_docs_with_termDB1) %in% c("n_docs_with_term_new","n_docs_with_term_existing_new"))]
head(n_docs_with_termDB1)

###########ADD to n_docs_with_termDB the new words in execution window ################
head(Tab_n_docs_with_term_new_execution)
colnames(Tab_n_docs_with_term_new_execution)<-c("wordlist","n_docs_with_term")
head(Tab_n_docs_with_term_new_execution)
Tab_n_docs_with_term_new_execution$totaldocs<-0
Tab_n_docs_with_term_new_execution$textlength<-""
Tab_n_docs_with_term_new_execution$language<-""
Tab_n_docs_with_term_new_execution$cutoff_notification_datetime<-""

head(Tab_n_docs_with_term_new_execution)

n_docs_with_termDB2<-rbind(n_docs_with_termDB1,Tab_n_docs_with_term_new_execution)
head(n_docs_with_termDB2)
nrow(n_docs_with_termDB2)
nrow(n_docs_with_termDB2)==nrow(n_docs_with_termDB1)+nrow(Tab_n_docs_with_term_new_execution)

###########ASSIGN VALUES to other parameters in the new n_docs_with_termDB ################

#totaldocs
totaldocs_current<-max(n_docs_with_termDB2$totaldocs)
totaldocs_new<-totaldocs_current+nrow(dtm_new)
n_docs_with_termDB2$totaldocs<-totaldocs_new


#cutoff_notification_datetime
new_cutoff_notification_datetime<-max_notification_datetime[1,1]
n_docs_with_termDB2$cutoff_notification_datetime<-new_cutoff_notification_datetime


#textlength
n_docs_with_termDB2$textlength<-"Long"

#textlength
n_docs_with_termDB2$language<-"English"


head(n_docs_with_termDB2)
tail(n_docs_with_termDB2)

#Convert datetime to datetime format from character
class(n_docs_with_termDB2$cutoff_notification_datetime)
n_docs_with_termDB2$cutoff_notification_datetime<-as.POSIXlt(n_docs_with_termDB2$cutoff_notification_datetime)
class(n_docs_with_termDB2$cutoff_notification_datetime)

head(n_docs_with_termDB2,20)


#### BE CAREFUL HERE WHEN WORKING ON SPANISH #####
nrow(n_docs_with_termDB2)
n_docs_with_termDB2$wordlist<-as.character(n_docs_with_termDB2$wordlist)
n_docs_with_termDB2<-n_docs_with_termDB2[(str_trim(n_docs_with_termDB2$wordlist)!=""),]
Encoding(n_docs_with_termDB2$wordlist) <- "UTF-8"
n_docs_with_termDB2<-n_docs_with_termDB2[!is.na(iconv(as.character(n_docs_with_termDB2$wordlist), "UTF-8", "ASCII")),]
nrow(n_docs_with_termDB2)

#Save to insert later in database
setwd(paste(baseWD,"\\Interim",sep=""))
save(n_docs_with_termDB2,file="n_docs_with_termDB2.rda")

setwd(paste(baseWD,"\\Backup",sep=""))
save(n_docs_with_termDB2,file=paste(currentTime_fName,"_EN_LONG_n_docs_with_termDB2.rda",sep=""))

#################################################################
## TF- IDF
#################################################################

##################Get "Tab_n_docs_with_term" for current set######################
Tab_n_docs_with_term<-n_docs_with_termDB2[(n_docs_with_termDB2$wordlist %in% wordList_execution),]
head(Tab_n_docs_with_term)

#Verify
nrow(Tab_n_docs_with_term)==length(wordList_execution)


#Find IDF for each term
Tab_IDF<-Tab_n_docs_with_term
ndocs<-max(as.numeric(as.character(Tab_n_docs_with_term$totaldocs)))
Tab_IDF$IDF<-log10(ndocs/(1+as.numeric(as.character(Tab_n_docs_with_term$n_docs_with_term))))
head(Tab_IDF[rev(order(Tab_IDF$IDF)),],20)
tail(Tab_IDF[rev(order(Tab_IDF$IDF)),],20)


#Set IDF of remove words to 0

head(Removewords)
nrow(Removewords)
removewordsList <- as.character(Removewords$Remove)
removewordsList<-c(removewordsList)
head(removewordsList,50)
tail(removewordsList,50)

head(Tab_IDF)
nrow(Tab_IDF)
nrow(Tab_IDF)==ncol(dtm_matrix_execution)

Tab_IDF[(Tab_IDF$wordlist %in% removewordsList),"IDF"]<-0
head(Tab_IDF[rev(order(Tab_IDF$IDF)),],20)
tail(Tab_IDF[rev(order(Tab_IDF$IDF)),],20)

#Check
nrow(Tab_IDF)
sum(Tab_IDF$IDF==0)


## create a term document matrix
dtm <- DocumentTermMatrix(LogData6_TFIDF)
ncol(dtm)
nrow(dtm)

#Remove sparse terms
words_to_keep<-colnames(dtm)
length(words_to_keep)
words_to_keep1<-words_to_keep[!(words_to_keep %in% incorrectListReduced)]
length(words_to_keep1)
removewordsList <- as.character(Removewords$Remove)
words_to_keep2<-words_to_keep1[!(words_to_keep1 %in% removewordsList)]
length(words_to_keep2)

dtm_dense<-dtm[, words_to_keep2]
ncol(dtm_dense)
nrow(dtm_dense)

#Garbage collection
gc()

#Convert to matrix
m <- as.matrix(dtm_dense)
rownames(m) <- 1:nrow(m)
head(m)

#Verify
ncol(m)
ncol(m)==length(freq_terms)

m[is.na(m)] <- 0
m[is.infinite(m)] <- 0
m[is.nan(m)] <- 0

head(m)


###GENERATE CLUSTER NAMES USING TF-IDF
#Grabage collection
gc()


#Set IDF Threshold to anything appearing in less than 5% of documents
IDF_Threshold<-log10(ndocs/(1+as.numeric(as.character(round(0.05*ndocs,0)))))

name_list<-character(nrow(m))
name_list_alpha<-character(nrow(m))


for(i in 1:nrow(m)){
	#Create Dataframe for 1 record
	list_m<-data.frame(cbind(colnames(m),as.character(m[i,])))
	#Keep only the words that have frequency>0
	list_m1<-list_m[as.character(list_m[,2])!=0,]
	#make list of such words
	list_m2<-as.character(list_m1[,1])
	#get IDF of thoese words
	list_m3<-Tab_IDF[(Tab_IDF$wordlist %in% list_m2),]
	#Keep only the words that have IDF > IDF_Threshold (Importance multiplying factor)
	list_m4_Tab<-list_m3[list_m3$IDF>IDF_Threshold,]
	#Just keep top 3
	list_m4<-as.character(head(list_m4_Tab[rev(order(list_m4_Tab$IDF)),"wordlist"],3))
	if(length(list_m4)>0){
		name_list[i]<-paste(list_m4,collapse="+")
		name_list_alpha[i]<-paste(list_m4[order(list_m4)],collapse="+")
	}
}

#Verify
length(name_list)
length(name_list_alpha)
nrow(m)

#Check how many named and not named
#Not Named
sum(str_trim(name_list)=="")
round(sum(str_trim(name_list)=="")/nrow(m),2)
#Named
sum(str_trim(name_list)!="")
round(sum(str_trim(name_list)!="")/nrow(m),2)
#Total
nrow(m)
#Verify
nrow(m)==sum(str_trim(name_list)=="")+sum(str_trim(name_list)!="")

DFText<-cbind(RowDF,as.data.frame(LogData6_TFIDF)$text,name_list,name_list_alpha)
colnames(DFText)<-c("notification_number","LogData6_TFIDF","name_list","name_list_alpha")
head(DFText)
nrow(DFText)
##################
#Checkpoint save
##################

setwd(paste(baseWD,"\\Interim",sep=""))
#save(DFText,file="DFText.rda")

#################################################################
## #0206- STOP ENTIRE SECTION CHANGED Text cleaning for naming using TF-IDF
#################################################################

#################################################################
## Combine the two output datasets
#################################################################
#eventsNamedPartsFinal<-get(load("eventsNamedPartsFinal.rda"))
#Output 1
head(eventsNamedPartsFinal)
Output1<-eventsNamedPartsFinal[,c("notification_number","clusterNameParts")]
Output1<-cbind(Output1,rep("Parts Lookup",nrow(Output1)))
colnames(Output1)<-c("notification_number","NLP_Cluster","NLP_Name_Source")
head(Output1)
nrow(Output1)

#Output 2
head(DFText)
nrow(DFText)
Output2<-DFText[,c("notification_number","name_list_alpha")]
Output2<-cbind(Output2,rep("TF-IDF",nrow(Output2)))
colnames(Output2)<-c("notification_number","NLP_Cluster","NLP_Name_Source")
head(Output2)
nrow(Output2)

#Combine the two
outputFinal<-rbind(Output1,Output2)
nrow(outputFinal)
nrow(outputFinal)==nrow(Output1)+nrow(Output2)

##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(outputFinal,file="outputFinal.rda")

#################################################################
## Split cluster name by '+'
#################################################################
sqldf("select count(distinct(NLP_Cluster)) from outputFinal")
nlpNameDF<-sqldf("select distinct(NLP_Cluster) from outputFinal")
head(nlpNameDF)

nlpNameList<-as.character(nlpNameDF$NLP_Cluster)
nlpNameList[1:6]
length(nlpNameList)

#Verify
length(nlpNameList)==sqldf("select count(distinct(NLP_Cluster)) from outputFinal")

#i=99998

TosplitList<-{}
FromsplitList<-{}

for(i in 1:length(nlpNameList)){


currentsplitList<-{}
current_List<-nlpNameList[i]

#current_List<-"a + b + c + d + e"
current_List_to_Split<-str_split(current_List,"\\+")[[1]]
currentsplitList<-c(currentsplitList,current_List_to_Split)

if(length(current_List_to_Split)>2){
	for(s1 in 1:length(current_List_to_Split)){
		s1_start<-s1+1
		if (s1_start<=length(current_List_to_Split)){
			for(s2 in s1_start:length(current_List_to_Split)){
				newname<-paste(current_List_to_Split[s1],current_List_to_Split[s2],sep="+")
				currentsplitList<-c(currentsplitList,newname)
			}
		}
	}
}
if(length(current_List_to_Split)>=4){
	for(s1 in 1:length(current_List_to_Split)){
		s1_start<-s1+1
		if (s1_start<=length(current_List_to_Split)){
			for(s2 in s1_start:length(current_List_to_Split)){
				s2_start<-s2+1
				if (s2_start<=length(current_List_to_Split)){
					for(s3 in s2_start:length(current_List_to_Split)){
						newname<-paste(current_List_to_Split[s1],current_List_to_Split[s2],current_List_to_Split[s3],sep="+")
						currentsplitList<-c(currentsplitList,newname)
					}
				}

			}
		}
	}
}

currentsplitList<-c(currentsplitList,current_List)
TosplitList<-c(TosplitList,currentsplitList)
FromsplitList<-c(FromsplitList,rep(current_List,length(currentsplitList)))

}


##################
#Checkpoint save
##################
setwd(paste(baseWD,"\\Interim",sep=""))
#save(TosplitList,file="TosplitList.rda")
#save(FromsplitList,file="FromsplitList.rda")
length(FromsplitList)
length(TosplitList)
length(FromsplitList)==length(TosplitList)

N_Gram_DF<-data.frame(cbind(FromsplitList,TosplitList))
colnames(N_Gram_DF)<-c("NLP_Cluster","TosplitList")
head(N_Gram_DF)
nrow(N_Gram_DF)

N_Gram_DF_Reduced<-N_Gram_DF[str_trim(as.character(N_Gram_DF$TosplitList))!="",]
nrow(N_Gram_DF_Reduced)

#Join with final output
head(outputFinal)
head(N_Gram_DF_Reduced)
nrow(outputFinal)

outputFinal2<-sqldf("select a.*,b.TosplitList from outputFinal a left join N_Gram_DF_Reduced b on a.NLP_Cluster=b.NLP_Cluster")
head(outputFinal2)
nrow(outputFinal2)

#Verify- only missing ones should be ones with misisng NLP_cluster
nrow(outputFinal2[str_trim(as.character(outputFinal2$TosplitList))=="",])
nrow(outputFinal2[str_trim(as.character(outputFinal2$NLP_Cluster))=="",])



#Verify
sqldf("select count(distinct(NLP_Cluster)) from outputFinal2")==sqldf("select count(distinct(NLP_Cluster)) from outputFinal")

#################################################################
## Clean final cluster names
#################################################################
#Remove spaces
outputFinal2$TosplitList<-gsub("\\s+", " ", str_trim(as.character(outputFinal2$TosplitList)))
head(outputFinal2)


outputFinal2$TosplitList2<-ifelse(substring(as.character(outputFinal2$TosplitList),1,1)=="+",
						str_trim(substring(as.character(outputFinal2$TosplitList),2)),
						str_trim(as.character(outputFinal2$TosplitList)))

#Verify
head(outputFinal2[outputFinal2$TosplitList!=outputFinal2$TosplitList2,])
nrow(outputFinal2[outputFinal2$TosplitList!=outputFinal2$TosplitList2,])

#Remove duplicates for the same noti
outputFinal3<-sqldf("select * from outputFinal2 
				where TosplitList2 is not null
				order by notification_number,TosplitList2,NLP_Name_Source")
head(outputFinal3)

nrow(outputFinal3)
First<-numeric(nrow(outputFinal3))

outputFinal3[,"notification_number"]<-str_trim(as.character(outputFinal3[,"notification_number"]))
outputFinal3[,"TosplitList2"]<-str_trim(as.character(outputFinal3[,"TosplitList2"]))
First[1]<-1

for(i in 2:nrow(outputFinal3)){
	if((outputFinal3[i,"notification_number"]!=outputFinal3[i-1,"notification_number"])
		|| (outputFinal3[i,"TosplitList2"]!=outputFinal3[i-1,"TosplitList2"])){
		First[i]<-1
	}
}

outputFinal4<-outputFinal3[First==1,]
nrow(outputFinal4)

#Verify 
sqldf("select count(distinct notification_number||TosplitList2) from outputFinal3")
sqldf("select count(distinct notification_number||TosplitList2) from outputFinal4")

head(outputFinal4)

#Keep only required columns
outputFinal5<-outputFinal4[,c("notification_number","NLP_Name_Source","TosplitList2")]
colnames(outputFinal5)<-c("notification_number","NLP_Name_Source","NLP_Cluster")
outputFinal5$NLP_Cluster<-toupper(outputFinal5$NLP_Cluster)
head(outputFinal5)

#Determine number of tokens
outputFinal5[,"NLP_Cluster"]<-as.character(outputFinal5[,"NLP_Cluster"])
N_Tokens<-numeric(nrow(outputFinal5))
for(i in 1:nrow(outputFinal5)){
	N_Tokens[i]<-length(str_split(outputFinal5[i,"NLP_Cluster"],"\\+")[[1]])

}
outputFinal5<-cbind(outputFinal5,N_Tokens)
head(outputFinal5)

#Get Notis for which there is no name
Noti_No_Cluster<-sqldf("select distinct(notification_number) from ProductionTable_English 
			where notification_number not in (select distinct(notification_number) from outputFinal5)")
head(Noti_No_Cluster)
nrow(Noti_No_Cluster)

Noti_No_Cluster2<-cbind(Noti_No_Cluster,
				rep("NONE",nrow(Noti_No_Cluster)),
				rep("MISCELLANEOUS",nrow(Noti_No_Cluster)),
				rep(0,nrow(Noti_No_Cluster)))
colnames(Noti_No_Cluster2)<-names(outputFinal5)
head(Noti_No_Cluster2)

#Append them
outputFinal6<-rbind(outputFinal5,Noti_No_Cluster2)
nrow(outputFinal6)==nrow(outputFinal5)+nrow(Noti_No_Cluster2)

head(outputFinal6)

#Bring Noti Description as well
outputFinal7<-sqldf("select a.*,b.notification_description from outputFinal6 a left join ProductionTable_English_Uniq2 b on a.notification_number==b.notification_number")
nrow(outputFinal6)
nrow(outputFinal7)

nrow(outputFinal6)==nrow(outputFinal7)

#Arrange columns
outputFinal7<-outputFinal7[,c("notification_number","notification_description","NLP_Cluster","N_Tokens","NLP_Name_Source")]
head(outputFinal7)
outputFinal7$notification_description<-""

#Add short or Long Text
outputFinal7$Textlength<-"Long"
outputFinal7$Language<-"English"
head(outputFinal7,100)
head(outputFinal7)

#Get rid of spaces
outputFinal7$NLP_Cluster<-gsub('\\s+', '',outputFinal7$NLP_Cluster)
head(outputFinal7,100)

#0508- Bring Noti date
head(outputFinal7,3)
head(ProductionTable_English,3)
Noti_DF<-unique(ProductionTable_English[,c("notification_number","notification_datetime")])
head(Noti_DF)
nrow(Noti_DF)<=nrow(ProductionTable_English[,c("notification_number","notification_datetime")])

nrow(outputFinal7)
outputFinal7<-merge(x=outputFinal7,y=Noti_DF,by="notification_number",all.x=TRUE)
nrow(outputFinal7)

head(outputFinal7,3)
#verify that all noti dates are in
print("verify Noti dates")
sum(is.na(outputFinal7$notification_datetime))
##################
##0206-Save the output to be used by combine code
##################
setwd(paste(baseWD,"\\Interim",sep=""))
save(outputFinal7,file="outputFinal7_English_Long.rda")

#######################################################################
####0206-Database Write
#######################################################################

#Set libraries
library("RPostgreSQL")
library("RJDBC")

#Set up Database Connection
drv_custom1 <- JDBC(driverClass = "com.amazon.redshift.jdbc41.Driver", classPath=classPath)
 
con <- dbConnect(drv_custom1,
                                                url="jdbc:redshift://dapcnvyranalytics.czsqals6k9uo.us-east-1.redshift.amazonaws.com:5439/conveyoranalytics",
                                                dbname = "conveyoranalytics",
                                                schemaname="common_tables",
                                                user = "dapappadmin",
                                                port = 5439,
                                                host = "dapcnvyranalytics.czsqals6k9uo.us-east-1.redshift.amazonaws.com",
                                                password = "Adm1n@aw$")
 
 
#Change the schema
dbGetQuery(con, "show search_path;")
dbSendUpdate(con, "set search_path to naturallanguageprocessing,public;")
dbGetQuery(con, "show search_path;")


#################### WRITE/UPDATE METADATA ######################################

### Append correct words table ###
setwd(paste(baseWD,"\\Interim",sep=""))

if (file.exists("correctListTab.rda")){
	correctListTab<-get(load("correctListTab.rda"))
	#0220
	correctListTab<-correctListTab[!is.na(iconv(as.character(correctListTab$new_correct_words), "UTF-8", "ASCII")),]
	returnWrite1<-dbWriteTable(con, "t_correct_words_list", 
             	value = correctListTab, overwrite=FALSE,append = TRUE, row.names = FALSE)
	returnCommit1<-dbCommit(con)
} else {
	returnWrite1<- TRUE
	returnCommit1<- TRUE
}

### Delete and Insert in t_n_docs_with_term ###
setwd(paste(baseWD,"\\Interim",sep=""))
n_docs_with_termDB2<-get(load("n_docs_with_termDB2.rda"))
head(n_docs_with_termDB2,20)

#0218
names(n_docs_with_termDB2)
n_docs_with_termDB2<-n_docs_with_termDB2[,c("wordlist","totaldocs","n_docs_with_term","textlength","language","cutoff_notification_datetime")]
head(n_docs_with_termDB2)

#Delete
returnDelete2<-dbSendUpdate(con,"delete from naturallanguageprocessing.t_n_docs_with_term where language='English' and textlength='Long';")
returnCommit2<-dbCommit(con)

#Insert
#0220
nrow(n_docs_with_termDB2)
n_docs_with_termDB2<-n_docs_with_termDB2[!is.na(iconv(as.character(n_docs_with_termDB2$wordlist), "UTF-8", "ASCII")),]
nrow(n_docs_with_termDB2)
returnWrite3<-dbWriteTable(con, "t_n_docs_with_term", 
             value = n_docs_with_termDB2, overwrite=FALSE,append = TRUE, row.names = FALSE)
returnCommit3<-dbCommit(con)

#################### UPDATE ERROR LOG ######################################

#Change the schema
dbGetQuery(con, "show search_path;")
dbSendUpdate(con, "set search_path to common_tables,public;")
dbGetQuery(con, "show search_path;")

# Change run status to SUCCESS if writing of results and metadata is successful
if(returnWrite1 && returnCommit1 && returnWrite3 && returnCommit3){
dbSendUpdate(con,"update common_tables.t_error_log
			set
				run_status='SUCCESS',
				run_status_message='',
				run_end_datetime=(select SYSDATE)
			 where code_description='English Long Text'
			 and run_start_datetime=(select max(run_start_datetime) from common_tables.t_error_log where code_description='English Long Text')")
dbCommit(con)
}

#######################################################################
####0206-Remove existing metadata and last output file
#######################################################################
setwd(paste(baseWD,"\\Interim",sep=""))
list.files(getwd())

if (file.exists("n_docs_with_termDB2.rda")){
	file.remove("n_docs_with_termDB2.rda")
}

if (file.exists("correctListTab.rda")){
	file.remove("correctListTab.rda")
}

#######################################################################
####0206-Finish: Disconnect
#######################################################################

#Disconnect from database
dbDisconnect(con)

#Disable Logging
sink(type="message")
sink(type="output")

close(zz_List)
close(zz_Log)
