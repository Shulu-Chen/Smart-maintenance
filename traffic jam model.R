library("plyr")
library("dplyr")

setwd("C:/Users/Lenovo/Desktop/PYTHON 售后")
data <- read.csv("traffic_jam.csv", header = TRUE)
data[,'排名.2']<-as.numeric(data[,'排名.2'])
data[,'排名.3']<-as.numeric(data[,'排名.3'])
high_time<-pnorm(data$排名.2, mean(data$排名.2), sd(data$排名.2))
low_time<-pnorm(data$排名.3, mean(data$排名.3), sd(data$排名.3))
high_time_score<--(high_time-0.5)/7.14+1.035
low_time_score<--(low_time-0.5)/7.14+0.965
data2<- cbind(data,high_time_score,low_time_score)
high_score <- unique(high_time_score)
low_score <- unique(low_time_score)
high_score <- sort(high_score,decreasing = TRUE)
low_score <- sort(low_score,decreasing = TRUE)
high_score2 <-c(high_score,0.97)
low_score2 <- c(low_score,0.90)
a<-c()
b<-c()
for (i in 1:5){
  a[i]<-(high_score2[i]+high_score2[i+1])/2
  b[i]<-(low_score2[i]+low_score2[i+1])/2
}
c<-c(1:5)
high_score_table<-cbind(c,a)
low_score_table<-cbind(c,b)
colnames(high_score_table)<-c("排名.2","high_score2")
colnames(low_score_table)<-c("排名.3","low_score2")
data2<-as.data.frame(data2)
high_score_table<-as.data.frame(high_score_table)
low_score_table<-as.data.frame(low_score_table)

score_table2<-left_join(data2,high_score_table,by="排名.2")
score_table2<-left_join(score_table2,low_score_table,by='排名.3')
write.csv(score_table2,'traffic_jam_distribution.csv')
