from bs4 import BeautifulSoup

# โครงสร้าง HTML เดิมที่คุณมี
html_template = '''
<div class="risk-box risk-critical"> 
    <div class="r-lbl">Risk Score</div> 
    <div class="r-val" id="acScore" style="color: rgb(153, 27, 27);">50</div> 
    <div class="r-badge badge-critical" id="acLevel">CHEATING</div> 
</div>
'''

soup = BeautifulSoup(html_template, 'html.parser')

# คะแนนใหม่ที่คุณต้องการใส่
new_score = 15

# หาตำแหน่งที่ต้องการใส่คะแนน แล้วแทนที่ข้อความเดิม
score_element = soup.find(id='acScore')
if score_element:
    score_element.string = str(new_score)
    
    # หากคะแนนน้อยลง สามารถปรับสีให้ปลอดภัยขึ้นได้ด้วย
    score_element['style'] = "color: rgb(22, 163, 74);" 

level_element = soup.find(id='acLevel')
if level_element:
    level_element.string = "NORMAL"
    level_element['class'] = ['r-badge', 'badge-safe']

# ดูผลลัพธ์ HTML ที่ถูกใส่ค่าใหม่แล้ว
print(soup.prettify())
