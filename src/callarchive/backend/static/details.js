const feedback = document.querySelector("#detail-feedback");
let pending;

function openDetail(dialog, closeUrl) {
  dialog.addEventListener("close", () => {
    dialog.remove();
    if (closeUrl) history.replaceState(history.state, "", closeUrl);
  }, { once: true });
  dialog.showModal();
}

const initialDetail = document.querySelector("#recording-detail");
if (initialDetail) openDetail(initialDetail, initialDetail.dataset.closeUrl);

document.addEventListener("click", async (event) => {
  const link = event.target.closest("a[data-open-detail]");
  if (!link || event.defaultPrevented || event.button !== 0 ||
      event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;

  event.preventDefault();
  pending?.abort();
  const request = new AbortController();
  pending = request;
  feedback.hidden = true;

  const url = new URL(link.href);
  url.searchParams.set("fragment", "1");
  try {
    const response = await fetch(url, { signal: request.signal });
    if (response.status === 401) {
      window.location.assign(link.href);
      return;
    }
    if (!response.ok) throw new Error("Detail request failed");
    const html = await response.text();
    if (request.signal.aborted) return;
    const doc = new DOMParser().parseFromString(html, "text/html");
    const dialog = doc.querySelector("#recording-detail");
    if (!dialog) throw new Error("Detail dialog missing");
    document.querySelector("#main").append(dialog);
    link.focus({ preventScroll: true });
    openDetail(dialog);
  } catch {
    if (!request.signal.aborted) {
      feedback.hidden = false;
      feedback.textContent = "暂时无法打开详情，请重试。";
    }
  } finally {
    if (pending === request) pending = undefined;
  }
});
