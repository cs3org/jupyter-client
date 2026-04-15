import React from 'react';
import { LabIcon } from '@jupyterlab/ui-components';

export const CERNBOX_LOGO_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="-2 -1 70 79"><polygon fill="#78b6e4" points="33.847,38.36 33.847,55.915 49.767,49.075 49.767,30.13 33.729,37.444"/><path fill="currentColor" d="M34.289 76.691c-9.242.0-16.483-16.843-16.483-38.346C17.806 16.845 25.047.0 34.289.0c9.24.0 16.482 16.845 16.482 38.346.0 21.503-7.242 38.345-16.482 38.345zm0-74.725c-7.868.0-14.517 16.657-14.517 36.38s6.648 36.38 14.517 36.38c7.866.0 14.515-16.657 14.515-36.38S42.155 1.966 34.289 1.966z"/><path fill="currentColor" d="M12.516 62.298c-5.683.0-9.647-1.669-11.467-4.834-4.607-8.014 6.382-22.688 25.024-33.407 10.521-6.051 21.732-9.666 29.989-9.666 5.681.0 9.646 1.669 11.467 4.835 2.272 3.956.822 9.654-4.098 16.045-4.733 6.159-12.165 12.322-20.925 17.36-10.525 6.053-21.734 9.667-29.99 9.667zM56.062 16.356c-7.927.0-18.771 3.516-29.015 9.406C9.958 35.593-1.167 49.661 2.753 56.487c1.831 3.18 6.32 3.846 9.763 3.846 7.925.0 18.771-3.516 29.011-9.409 8.541-4.91 15.768-10.896 20.347-16.85 4.338-5.639 5.774-10.693 3.95-13.873-1.829-3.176-6.32-3.845-9.762-3.845z"/><path fill="currentColor" d="M56.063 62.298c-8.258.0-19.471-3.614-29.99-9.667C17.31 47.593 9.88 41.43 5.145 35.271.229 28.88-1.227 23.182 1.049 19.226c1.819-3.166 5.784-4.835 11.467-4.835 8.257.0 19.466 3.615 29.991 9.666 18.637 10.72 29.629 25.394 25.022 33.407C65.709 60.629 61.743 62.298 56.063 62.298zM12.516 16.356c-3.442.0-7.932.669-9.763 3.845-1.824 3.18-.386 8.234 3.951 13.873 4.582 5.953 11.804 11.939 20.344 16.85 10.24 5.894 21.085 9.409 29.015 9.409h.001c3.443.0 7.932-.666 9.76-3.846 3.924-6.826-7.203-20.895-24.297-30.725C31.287 19.872 20.44 16.356 12.516 16.356z"/><polygon fill="#27aae1" points="34.816,20.646 19.294,29.13 33.599,37.566 48.683,29.104"/><path fill="currentColor" d="M33.599 38.549c-.167.0-.333-.044-.484-.132l-14.308-8.434c-.303-.18-.486-.512-.481-.867.006-.357.201-.681.512-.852l15.521-8.485c.297-.163.664-.156.955.026l13.865 8.457c.295.181.478.507.471.86-.007.351-.195.674-.503.841l-15.078 8.464C33.92 38.509 33.762 38.549 33.599 38.549zM21.275 29.163l12.336 7.274 13.134-7.369L34.789 21.78 21.275 29.163z"/><rect fill="currentColor" x="32.746" y="37.566" width="1.965" height="16.848"/><polygon fill="currentColor" points="33.847,38.36 19.294,30.13 19.294,49.075 33.847,55.915"/></svg>';

export const spacesIcon = new LabIcon({
  name: '@cs3org/cs3-jupyter-client:spaces',
  svgstr: `<svg xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path xmlns="http://www.w3.org/2000/svg" d="M22 12.999V20C22 20.5523 21.5523 21 21 21H13V12.999H22ZM11 12.999V21H3C2.44772 21 2 20.5523 2 20V12.999H11ZM11 3V10.999H2V4C2 3.44772 2.44772 3 3 3H11ZM21 3C21.5523 3 22 3.44772 22 4V10.999H13V3H21Z"></path></svg>`
});

export const cernboxIcon = new LabIcon({
  name: '@cs3org/cs3-jupyter-client:cernbox',
  svgstr: CERNBOX_LOGO_SVG
});

export const shareIcon = new LabIcon({
  name: '@cs3org/cs3-jupyter-client:shares',
  svgstr: `<svg xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path xmlns="http://www.w3.org/2000/svg" d="M13 14H11C7.54202 14 4.53953 15.9502 3.03239 18.8107C3.01093 18.5433 3 18.2729 3 18C3 12.4772 7.47715 8 13 8V3L23 11L13 19V14Z"></path></svg>`
});

export const SharedWithMeIcon = (
  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 24 24">
    <path d="M13 14H11C7.54202 14 4.53953 15.9502 3.03239 18.8107C3.01093 18.5433 3 18.2729 3 18C3 12.4772 7.47715 8 13 8V3L23 11L13 19V14Z" />
  </svg>
);

export const SharedByMeIcon = (
  <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 24 24">
    <path d="M11 14H13C16.458 14 19.4605 15.9502 20.9676 18.8107C20.9891 18.5433 21 18.2729 21 18C21 12.4772 16.5228 8 11 8V3L1 11L11 19V14Z" />
  </svg>
);

export const SharedPubliclyIcon = (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="16"
    height="16"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
    <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
  </svg>
);

export const ViewIcon = (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
    <circle cx="12" cy="12" r="3" />
  </svg>
);

export const EditIcon = (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M17 3a2.83 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3z" />
  </svg>
);

export const UserEmoji = '\ud83d\udc64';
export const GroupEmoji = '\ud83d\udc65';

export const TrashIcon = (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="3 6 5 6 21 6" />
    <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" />
    <path d="M10 11v6" />
    <path d="M14 11v6" />
    <path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2" />
  </svg>
);
