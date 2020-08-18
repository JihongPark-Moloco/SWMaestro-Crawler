# Crawler Repository
MUNA 팀의 CreateTrend 프로젝트에서 필요한 데이터 수집에 활용되는
크롤러 레포지토리입니다.

## Crawlers
#### Youtube
유튜브 트렌드 정보와 분석 인사이트 제공을 위한 유튜브 크롤러입니다.
#### Vling
카테고리별 유튜브 채널을 분류하기 위해 참고할 데이터인 블링의 사이트를 크롤링하는 크롤러입니다.
#### NoxInfluencer
추가적인 데이터 수집과 빠른 기반 데이터 확보를 위한 NoxInfluencer 크롤러입니다.

## File Structure
##### Nox_Crawler.py  
셀레니움 기반의 녹스 인플루언서 크롤러입니다.  
#####Vling_Crawler.py
셀레니움 기반의 블링 크롤러입니다.  
#####snedToRabitMQ.py
크롤러 동작시 필요한 정보를 메세지 큐에 저장하는 일회성 소스입니다.
#####Data_API_Youtube_Crawler      
YouTube Data API V3와 프록시를 활용한 안정적인 유튜브 크롤러입니다. (현재 프로젝트에서 사용)
#####Selenium_YouTube_Crawler
Selenium을 기반으로 크롤링하는 이전 버전의 크롤러입니다. 불안정함으로 인해 사용 보류중입니다.

## 
