window.apiGet = async function (path, params = {}) {
    const url = new URL(path, window.location.origin);

    Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null) {
            url.searchParams.set(key, String(value));
        }
    });

    const response = await fetch(url);
    const result = await response.json().catch(() => ({}));

    if (!response.ok) {
        throw new Error(result.error || `Request failed (${response.status})`);
    }

    return result;
};