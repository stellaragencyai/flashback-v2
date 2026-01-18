from pathlib import Path
import re

p = Path(r"app/dashboard/templates/dashboard.html")
s = p.read_text(encoding="utf-8", errors="ignore")

if "bindActionButtons" in s:
    print("PATCH_NOOP: JS already wired")
    raise SystemExit(0)

js = r'''
// PART 3: Action buttons (safe no-op)
function bindActionButtons(){
  document.querySelectorAll(".actionRow .actBtn").forEach(btn=>{
    btn.addEventListener("click", async ()=>{
      const row = btn.closest(".actionRow");
      const acct = row.getAttribute("data-acct");
      const action = btn.getAttribute("data-action");

      try{
        const resp = await fetch("/api/action",{
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({ account: acct, action })
        });
        const j = await resp.json();
        if(j.ok){
          alert(`Action '${action}' accepted for ${acct}`);
        } else {
          alert(`Action failed: ${j.error||"unknown"}`);
        }
      }catch(e){
        alert("Network error");
      }
    });
  });
}

bindActionButtons();
'''

s = s.replace("</script>", js + "\n</script>")
p.write_text(s, encoding="utf-8")
print("OK: Action buttons wired to backend")
