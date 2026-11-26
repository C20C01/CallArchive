const date = document.querySelector("#call-date");
if (date) {
  for (const step of document.querySelectorAll("[data-day-step]")) {
    step.addEventListener("click", () => {
      if (!date.value) {
        const now = new Date();
        date.valueAsNumber = Date.UTC(now.getFullYear(), now.getMonth(), now.getDate());
      }
      date.stepUp(Number(step.dataset.dayStep));
      date.form.requestSubmit();
    });
  }
}

const groups = new Map();
for (const link of document.querySelectorAll("[data-filter-group]")) {
  const key = JSON.stringify([link.dataset.filterGroup, link.dataset.filterValue]);
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(link);
}
for (const links of groups.values()) {
  function highlight() {
    const active = links.some((link) => link.matches(":hover, :focus-visible"));
    for (const link of links) link.classList.toggle("phone-match", active);
  }

  for (const link of links) {
    link.addEventListener("mouseenter", highlight);
    link.addEventListener("mouseleave", highlight);
    link.addEventListener("focusin", highlight);
    link.addEventListener("focusout", () => queueMicrotask(highlight));
  }
}
