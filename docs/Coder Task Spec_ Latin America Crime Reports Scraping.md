### **Overview**

The [Maritime Information Fusion Centre](https://www.dicapi.mil.pe/ifc-latam-peru/publicaciones/semanal) out of Peru maintains weekly crime reports from 2023 for several Latin American countries. These reports are in PDF form, 52 per year, and contain a summary of the week’s crimes and a small blurb about each one. Within the blurb, a link is included to the original news source. 

We need to search through each of these reports, find the original source links, and look for any mentions of our ships or plants of interest. 

---

### **Objective(s)**

* Produce a spreadsheet that contains the link to any mentions of our ships or plants of interest.

---

### **Requirements & Suggested Approach**

1. Use this list of [plants, topics, and ships](https://docs.google.com/spreadsheets/d/1_TeCQ0qkV2AXbrVCouL2A00PAgLUuDTxqBEkdIEXBmo/edit?usp=sharing) that we want to search for  
   * We only care about the link to the original news source, not the weekly crime report.   
   * No need to summarize or translate the source  
   * Place a list of links in the “Crime report links” field for any given row  
2. You are, as always, welcome to solve this problem anyway you choose. My best idea is to extract the links from the PDFs, put them in one big spreadsheet, loop through each one and parse the HTML, then search that HTML for any of the plant or ship names.   
3. The crime blurbs are not structured, so   
   * You will likely need to play around with cleaning the plant names, e.g. removing “S.A.”, “S.R.L.” etc.   
   * For ships, they will likely only mention the name but it might be worth searching by IMO or National Registration Number

---

**Deliverables**

1. **Completed spreadsheet with linked crimes for each of the 6 tabs in the sheet:**  
   * FMFO plants  
   * Topics  
   * Vessel Owners  
   * Vessels in Ecuador  
   * Vessels in Peru  
   * Vessels in Chile  
2. **Documentation**  
   * Includes all relevant code files  
   * **Excludes all API keys and passwords** so the code can be shared publicly.

