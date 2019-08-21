setwd("C:/Users/Lenovo/Desktop/2画像")
weather<-read.csv("天气评分2018new2.csv")
library("plyr")
library("dplyr")
weather_table <- weather[,1:8]
rain <- (weather$rainscore1+weather$rainscore2)/2
snow <- (weather$snowscore1+weather$snowscore2)/2
bty_temp <- (weather$Hbatteryscore+weather$Lbatteryscore)/2
brk_temp <- (weather$Hbreakscore+weather$Lbreakscore)/2
igt_temp <- (weather$Hignscore+weather$Lignscore)/2
weather_table <- cbind(weather_table,rain,snow,weather$hazescore,
                       weather$windscore,bty_temp,brk_temp,igt_temp)
dis_rain<-pnorm(weather_table$rain, mean(weather_table$rain), sd(weather_table$rain))
dis_snow<-pnorm(weather_table$snow,mean(weather_table$snow), sd(weather_table$snow))
dis_haze<-pnorm(weather_table$`weather$hazescore`,mean(weather_table$`weather$hazescore`), sd(weather_table$`weather$hazescore`))
dis_wind<-pnorm(weather_table$`weather$windscore`,mean(weather_table$`weather$windscore`), sd(weather_table$`weather$windscore`))
dis_btytemp<-pnorm(weather_table$bty_temp,mean(weather_table$bty_temp), sd(weather_table$bty_temp))
dis_brktemp<-pnorm(weather_table$brk_temp,mean(weather_table$brk_temp), sd(weather_table$brk_temp))
dis_igttemp<-pnorm(weather_table$igt_temp,mean(weather_table$igt_temp), sd(weather_table$igt_temp))

dis_rain_score<--(dis_rain-0.5)/2.5+1
dis_snow_score<--(dis_snow-0.5)/2.5+1
dis_haze_score<--(dis_haze-0.5)/2.5+1
dis_wind_score<--(dis_wind-0.5)/2.5+1
dis_btytemp_score<--(dis_btytemp-0.5)/2.5+1
dis_brktemp_score<--(dis_brktemp-0.5)/2.5+1
dis_igttemp_score<--(dis_igttemp-0.5)/2.5+1

dis_rainsnow <- cbind(dis_rain_score,dis_snow_score)
dis_windhaze <- cbind(dis_wind_score,dis_haze_score)
dis_rainsnow_score<-apply(dis_rainsnow,1,max)
dis_windhaze_score<-apply(dis_windhaze,1,max)
weather_table_dis <- cbind(weather_table[,1:8],dis_rainsnow_score,dis_windhaze_score,
                           dis_btytemp_score,dis_brktemp_score,dis_igttemp_score)

write.csv(weather_table_dis,'weather_table_dis.csv')