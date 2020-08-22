# Crawler Repository
## About The Project
MUNA 팀의 CreateTrend 프로젝트에서 유튜브 트렌드 정보와 분석 인사이트 제공을 위한 유튜브 크롤러입니다.
### Collecting Items
#### 채널 정보
- 채널 이름
- 채널 설명
- 개시 날짜
- 구독자수
- 채널 조회수

#### 영상 정보
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
* [Strom Proxies](https://stormproxies.com/)

### PipeLine
![image](https://13.125.91.162/swmaestro/muna-3/raw/master/images/crawler_pipeline.png)  
1. 관리자가 직접 선정한 채널들의 channel_id를 수동으로 DataBase에 입력해줍니다.
2. 정기적으로 크롤러가 작동하며 신규 입력된 채널의 채널 정보를 수집합니다.
3. DB에 가지고있는 모든 채널 정보를 갱신합니다.
4. 채널에 업로드된 신규 영상이 존재할경우 신규 영상 정보를 수집합니다.
5. DB에 존재하는 모든 영상들의 영상 및 댓글 정보를 갱신합니다.

### Crawler Structure
#### YouTube Crawler
New_YouTube_Crawler.py에 선언된 YouTube_Crawler 클래스를 통해 활용가능합니다.  
함수는 다음의 종류가 존재합니다.
- insert_channel_info(channel_id)  
입력받은 channel_id를 통해 DB에 신규 채널 정보를 입력합니다.
- update_channel_info(channel_id)  
입력받은 channel_id의 채널 정보를 갱신합니다. 
- update_video_and_comment(video_id)  
입력받은 video_id의 영상 정보를 갱신하고 댓글을 수집합니다.
- update_video_info(upload_id, interval_day=30):  
입력받은 upload_id를 통해 신규 비디오의 정보를 DB에 수집합니다.