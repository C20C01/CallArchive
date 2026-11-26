const form = document.querySelector("#bulk-remove");
const selectPage = document.querySelector("#select-page");
const removeSelected = document.querySelector("#remove-selected");

if (form && selectPage) {
  const entries = Array.from(document.querySelectorAll('input[form="bulk-remove"]:enabled'));
  const dialog = document.querySelector("#remove-dialog");
  const selectedCount = document.querySelector("#selected-count");

  function syncSelection() {
    const count = entries.filter((entry) => entry.checked).length;
    selectPage.disabled = entries.length === 0;
    selectPage.checked = count > 0 && count === entries.length;
    selectPage.indeterminate = count > 0 && count < entries.length;
    removeSelected.disabled = count === 0;
    removeSelected.textContent = count ? `移除所选（${count}）` : "移除所选";
    selectedCount.textContent = count;
  }

  for (const entry of entries) entry.addEventListener("change", syncSelection);
  selectPage.addEventListener("change", () => {
    for (const entry of entries) entry.checked = selectPage.checked;
    syncSelection();
  });
  removeSelected.addEventListener("click", () => dialog.showModal());
  dialog.addEventListener("click", (event) => {
    if (event.target.closest("[data-cancel-remove]")) dialog.close();
  });
  window.addEventListener("pageshow", syncSelection);
  syncSelection();
}
