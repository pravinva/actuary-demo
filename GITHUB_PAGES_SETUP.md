# GitHub Pages Setup

## ✅ Code Pushed to GitHub Successfully!

**Repository:** https://github.com/pravinva/actuary-demo
**Branch:** main

---

## 🚀 Enable GitHub Pages (2 minutes)

### Step 1: Navigate to Repository Settings
1. Go to: https://github.com/pravinva/actuary-demo
2. Click **"Settings"** tab (top right)

### Step 2: Enable GitHub Pages
1. Scroll down to **"Pages"** section (left sidebar)
2. Under **"Source"**, select:
   - Branch: **main**
   - Folder: **/ (root)**
3. Click **"Save"**

### Step 3: Wait for Deployment (1-2 minutes)
GitHub will automatically build and deploy your site.

### Step 4: Access Your Interactive Dashboard
Your demo will be available at:

```
https://pravinva.github.io/actuary-demo/
```

Or check the "Pages" section in Settings for the exact URL.

---

## 📊 What's Published

Your GitHub Pages site includes:

### **index.html** - Interactive Dashboard
- Complete demo overview with live statistics
- 5 tabs: Overview, Genie Queries, Data Quality, Time Travel, Architecture
- All Genie query results with real data
- DQ expectations and comparisons
- Time travel use cases and SQL examples
- Databricks vs SAS/Excel comparison

### **Supporting Documentation**
- `CUSTOMER_AGENDA.md` - What's covered in the demo (for pre-call prep)
- `DEMO_COMPLETE.md` - Complete 30-minute demo script
- `FINAL_DEPLOYMENT_SUMMARY.md` - Technical deployment details
- `README.md` - Repository overview

### **Demo Files**
- 4 Dashboard SQL queries (1-4_*.sql)
- Genie setup guide and sample questions
- Python scripts for demo execution

---

## 🎨 How to Use the Interactive Dashboard

### **For Pre-Demo Prep:**
1. Share GitHub Pages URL with stakeholders before the meeting
2. They can explore at their own pace
3. Review actual Genie query results with real data

### **During Demo:**
1. Open interactive dashboard on second screen
2. Click through tabs as you demonstrate live in Databricks
3. Reference visualizations and comparisons

### **Post-Demo:**
1. Send GitHub Pages URL as follow-up
2. Stakeholders can revisit key concepts
3. Use as reference during pilot planning

---

## 📝 Customization (Optional)

### Update Demo Statistics
Edit `index.html` and update the stats-grid section:

```html
<div class="stat-card">
    <div class="stat-number">YOUR_VALUE</div>
    <div class="stat-label">Your Metric</div>
</div>
```

### Add Your Logo
1. Add logo file to repository (e.g., `logo.png`)
2. Update header in `index.html`:

```html
<header>
    <img src="logo.png" alt="Company Logo" style="max-width: 200px;">
    <h1>🏦 Finance Actuarial Demo</h1>
    ...
</header>
```

### Change Color Scheme
Update CSS variables in `<style>` section:

```css
/* Change primary color from Databricks red */
header {
    background: linear-gradient(135deg, #YOUR_COLOR 0%, #YOUR_COLOR_DARK 100%);
}
```

---

## 🔄 Push Updates

After making changes:

```bash
git add .
git commit -m "Update demo content"
git push origin main
```

GitHub Pages will automatically rebuild (takes 1-2 minutes).

---

## 📱 Share with Customers

### Option 1: Direct Link
```
Hey [Customer],

Check out our interactive Finance Actuarial demo:
https://pravinva.github.io/actuary-demo/

This shows exactly what we'll cover in our session tomorrow:
- Live Genie query results
- Data quality approach
- Time travel for audit
- Complete architecture

See you at [TIME]!
```

### Option 2: QR Code
Generate a QR code at https://www.qr-code-generator.com/ pointing to your GitHub Pages URL.
Print it on presentation materials or display during demos.

### Option 3: Embed in Email
```html
<p>Explore our <a href="https://pravinva.github.io/actuary-demo/" style="color: #FF3621; font-weight: bold;">Interactive Finance Actuarial Demo</a></p>
```

---

## 🎯 Demo URLs Quick Reference

| Resource | URL |
|----------|-----|
| **Interactive Dashboard** | https://pravinva.github.io/actuary-demo/ |
| **GitHub Repo** | https://github.com/pravinva/actuary-demo |
| **Databricks Workspace** | https://e2-demo-field-eng.cloud.databricks.com |
| **SDP Pipeline** | https://e2-demo-field-eng.cloud.databricks.com/#joblist/pipelines/2a52433e-beb8-446d-9091-e40854f9bd88 |
| **Catalog** | actuary_corpfin |
| **Warehouse** | 4b9b953939869799 |

---

## ✅ Verification

Once GitHub Pages is enabled, verify:

1. **Dashboard loads**: Visit the GitHub Pages URL
2. **Navigation works**: Click through all 5 tabs
3. **Tables render**: Check Genie query results display correctly
4. **Styling correct**: Colors, fonts, and layout look good
5. **Mobile responsive**: Test on phone/tablet

---

## 🆘 Troubleshooting

### "404 - File not found"
- Wait 2-3 minutes after enabling Pages
- Check that Branch is set to "main" and Folder to "/ (root)"
- Verify `index.html` is in the root of the repository

### "Styles not loading"
- All CSS is inline in `index.html`, so this shouldn't happen
- Hard refresh: Ctrl+F5 (Windows) or Cmd+Shift+R (Mac)

### "Page shows old content"
- GitHub Pages cache: Wait 2-5 minutes
- Clear browser cache
- Try incognito/private window

---

## 🎉 Success!

Your interactive Finance Actuarial demo is now live and shareable!

**Next Steps:**
1. Test the GitHub Pages URL
2. Review the CUSTOMER_AGENDA.md for talking points
3. Practice the demo flow from DEMO_COMPLETE.md
4. Share with stakeholders

---

**Built Date:** 2026-03-17
**Repository:** https://github.com/pravinva/actuary-demo
**Pages URL:** https://pravinva.github.io/actuary-demo/
