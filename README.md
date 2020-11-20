# Crawler Repository
MUNA 팀의 CreateTrend 프로젝트에서 유튜브 정보를 수집하기 위한 크롤러 레포입니다.
## 크롤러 개요
### 채널 정보
- 채널 이름
- 채널 설명
- 개시 날짜
- 채널 구독자수
- 채널 조회수

### 영상 정보
- 영상 제목
- 영상 설명
- 영상 업로드 날짜
- 영상 추천/비추천 수
- 영상 조회수
- 영상 댓글 (영상별 추천순 상위 100개)
- 영상 댓글별 추천수 

### Built With
현 프로젝트는 다음의 주요 서비스를 통해 개발되었습니다.
* [YouTube Data API v3](https://developers.google.com/youtube/v3)
* [Rotating Proxies](https://rotatingproxies.com/)

### PipeLine
![image](https://user-images.githubusercontent.com/50457791/99808884-ddb16500-2b84-11eb-87da-bd64c2ec8ac4.png)  
크롤러 동작은 3단계로 이루어집니다.
1. 채널 정보 갱신  
  채널의 조회수, 활성화 여부, 구독자수등의 정보를 업데이트합니다.
2. 신규 영상 수집  
  채널의 신규 영상 정보를 가져옵니다.
3. 영상 정보 수집  
  영상의 조회수, 댓글, 좋아요수, 싫어요수등을 수집합니다.

## 크롤러 상세 설명
### 디렉토리 구조  
- /  
  크롤러 동작의 구동부입니다.  
  supervisor를 통해 자동화된 수집 크롤러 동작을 실행합니다.  
  
- /Data_API_YouTube_Crawler  
  YouTube Data API v3를 이용하는 크롤러입니다.  
  Selenium Crawler의 한계를 극복하기 위해 개발되었습니다.  
  실질적으로 활용하는 소스들이 포함되어있습니다.  
  
- /Selenium_YouTube_Crawler  
  현재는 사용하지 않는 Selenium 기반의 크롤러 소스가 포함되어있습니다.  
  
- /Utils  
  NoxInfluencer, Vling 페이지의 유튜버 목록을 크롤링 하기 위한 소스입니다.  
  최초 유튜버들을 카테고리별로 수집하기 위해 활용되었습니다.  

### 핵심 소스 설명
- supervisor.py  
  전체 수집 프로세스를 자동화 작업으로 수행합니다.  
  각 스텝마다 sendToRabbitMQ로 메세지큐에 필요한 채널/영상 ID를 넣고  
  메세지큐를 소비하는 프로세스를 subprocess로 실행합니다.  
  또한 메세지큐를 감시해 모든 메세지가 소비되면 다음 동작을 스텝을 실행합니다.  
  
- sendToBabbitMQ.py  
  서버 메세지큐에 필요한 데이터 정보를 삽입합니다.  
  삽입하는 데이터로는 channel_id, upload_id, video_id 등이 있으며 각 스텝 진행상황에 맞게 삽입합니다.  
  
- /Data_API_YouTube_Crawler/New_YouTube_Crawler_Channel_Updater.py  
  YouTube Data API를 활용해 채널 정보를 업데이트합니다.  
  구독자수, 조회수, 채널 활성화 여부를 수집해 DB에 업데이트합니다.  
  
- /Data_API_YouTube_Crawler/New_YouTube_Crawler_New_Channel_Inserter.py  
  YouTube Data API를 활용해 신규 채널 정보를 DB에 저장합니다.  
  신규 채널에만 적용하며 이후는 channel_updater를 이용해 채널 정보를 업데이트할 수 있습니다.  
  
- /Data_API_YouTube_Crawler/New_YouTube_Crawler_New_Video_Inserter.py  
  YouTube Data API를 활용해 채널의 신규 영상 정보를 DB에 저장합니다.  
  채널의 upload_id로 부터 영상 정보를 가져오고 DB에 존재하는 영상과 만날때까지 영상 정보를 삽입합니다.  
  interval_day를 이용해 영상 수집 범위를 조정할 수 있습니다.  
  
- /Data_API_YouTube_Crawler/New_YouTube_Crawler_proxy_APP.py  
  Rotating Proxy 서비스를 이용해서 유튜브 영상 정보를 수집합니다.  
  영상의 조회수, 댓글, 좋아요수, 싫어요수등을 수집합니다.  
  유튜브의 보안 정책을 회피하기 위해 backconnect proxy 서비스를 이용해서 매 프로세스마다 각기 다른 IP를 가지고  
  크롤러 동작을 수행합니다.




## 활용 서비스
### YouTube Data API v3  
유튜브에서 제공하는 어플리케이션을 위한 API입니다.  
유료 서비스는 존재하지 않으며 계정당 일일 허용량이 매우 제한적인 것으로 유명합니다.  
또한 허용량 증설 요청에 대해 매우 부정적입니다.  
  
-> 이를 해결하기 위해 모든 API 함수의 Document를 철저히 분석해 최적의 프로세스를 구현했습니다.  
  
#### 활용 API
##### channels  
[API channels](https://developers.google.com/youtube/v3/docs/channels?hl=ko)
채널의 정보를 수집하기 위해 channel_id 값을 파라미터로 호출합니다.  
##### playlistItems  
[API playlistItems](https://developers.google.com/youtube/v3/docs/playlistItems?hl=ko)
채널별 고유 upload_id를 이용해 채널의 모든 영상 정보를 가져옵니다.  

## Authors
- **박지홍(qkrwlghddlek@naver.com)**
