X1 = read.csv("probki.csv",header=TRUE, sep=";")

cat("V;Mean;Median;Quantile_10;TopMinMean;MB_1;MB_2",file="pomiary_aktywa.csv", sep="\n")

T<-1000000
Mean<-replicate(6,0)
Median<-replicate(6,0)
Quantile_10<-replicate(6,0)
TopMinMean<-replicate(6,0)
MB_1<-replicate(6,0)
MB_2<-replicate(6,0)

for (column in 1:6){
  Mean[column]<- mean(X1[,column])
  Median[column] <- median(X1[,column])
  Quantile_10[column] <-quantile(X1[,column], probs=0.1)
  TopMinMean[column]<- mean(head(sort(X1[,column]),0.1*T))
  MB_1[column]<- Mean[column] - sum(abs(rep(Mean[column], T) - X1[,column]))/2/T
  temp_sum<-0
  GM<-GiniMd(X1[,column])*(T-1)
  MB_2[column]<-Mean[column] - GM/2/T
  cat(c(column,Mean[column],Median[column],Quantile_10[column]
        ,TopMinMean[column],MB_1[column], MB_2[column])
      ,file = "pomiary_aktywa.csv", sep=";", append=TRUE)
  cat("\n" ,file = "pomiary_aktywa.csv", append=TRUE)
}
